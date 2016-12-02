# -*- coding:utf-8 -*-
import os
import json
from pyspark import SparkContext
from datetime import date, datetime, timedelta
from optparse import OptionParser
import MySQLdb
import urllib
from redis import Redis

TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'
LIST_ARTICLE_CLICK = '020103'
ARTICLE_LIKE = '020204'
ARTICLE_COLLECT = '020205'
ARTICLE_COMMENT = '020207'
VALID_EVENTID_LST = [ARTICLE_LIKE,
                     LIST_ARTICLE_CLICK,
                     ARTICLE_COMMENT]
NOW = datetime.now()
CURRENT_AVAILABLE_CNT = 200
CHANNEL_THRESHOLD_DCT = {
        # channelId, (cntThr, dayThr)
        10001: (10, 1.5),   # hot
        10002: (2, 2),    # world
        10003: (2, 1),    # sports
        10004: (2, 1),    # entertainment
        10005: (0.1, 2),  # games
        10006: (0.1, 2),  # lifestyle
        10007: (0.1, 2),  # business
        10008: (0.1, 2),  # sci&tech
        10009: (0.1, 2),  # opinion
        10010: (2, 1),    # national
        10013: (0.1, 2),  # NBA
    }
REDIS_POPULAR_NEWS_PREFIX = 'BA_POPULAR_NEWS_%i'
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
TMP_DIR = '/home/work/limeng/tmp'

def getSpanRdd(start_date, end_date, withToday=False):
    fileLst = []
    if start_date >= end_date:
        return fileLst
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
        withToday = True
    if withToday:
        res = os.popen('%s fs -ls %s' % (HADOOP_BIN, TODAY_LOG_DIR)).readlines()
        if len(res):
            fileLst.append(TODAY_LOG_DIR)

    # append history's log
    cur_date = start_date
    while cur_date < end_date:
        if cur_date >= date.today():
            cur_date += timedelta(days=1)
            continue
        curDir = os.path.join(HISTORY_LOG_DIR, \
                cur_date.strftime('%Y/%m/%d'), 'useraction.log-*')
        res = os.popen("%s fs -ls %s" % (HADOOP_BIN, curDir)).readlines()
        if len(res) != 0:
            fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getTransferTime(timestamp):
    if timestamp:
        timestamp = datetime.fromtimestamp(timestamp)
        return timestamp
    else:
        return None

def getNewsMeta(sc):
    conn = MySQLdb.connect(host='10.8.22.123',
                           user='banews_w',
                           passwd=urllib.quote('MhxzKhl-Happy'),
                           port=3306,
                           db='banews')
    conn.autocommit(True)
    cursor = conn.cursor()
    sqlCmd = '''
SELECT
  `url_sign`,
  `channel_id`,
  `fetch_time`
FROM
  `tb_news`
WHERE
  (
    `channel_id` NOT IN (10011, 10012, 30001)
  ) AND (`is_visible` = 1);
'''
    cursor.execute(sqlCmd)
    resLst = cursor.fetchall()
    metaRdd = sc.parallelize(resLst, 64).map(
            lambda (urlSign, chId, fetchTime): (urlSign, (int(chId),
                getTransferTime(fetchTime)))
        ).filter(
            lambda (newsId, (chId, fetchTime)): ((NOW - fetchTime).total_seconds() / \
                    (60 * 60.)) <= (24 * CHANNEL_THRESHOLD_DCT.get(chId, (1, 1))[1])
        )
    return metaRdd

def calcSco(eventId, fetchTimeStamp):
    span = (NOW - fetchTimeStamp).total_seconds() / (60 * 60.)
    if eventId == ARTICLE_LIKE:
        sco = 3.
    elif eventId == ARTICLE_COMMENT:
        sco = 3.
    else:
        sco = 1.
    sco *= pow(0.5, span / 8)
    return sco

def cleanDirectory():
    today = date.today()
    for curFileName in os.listdir(TMP_DIR):
        vals = curFileName.strip().split('_', 1)
        if len(vals) != 2:
            continue
        curDate = datetime.strptime(vals[0], '%Y-%m-%d').date()
        if (today - curDate).days >= 2:
            os.popen('rm -rf %s' % os.path.join(TMP_DIR,
                curFileName))
    return None

def calcNewsUV(sc, start_date, end_date):
    newsMetaRdd = getNewsMeta(sc)
    fileLst = getSpanRdd(start_date, end_date)
    uvRdd = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: json.loads(dctStr)
        ).map(
            lambda attrDct: (attrDct.get('event-id'),
                             attrDct.get('did'),
                             attrDct.get('news_id'),
                             getTransferTime(float(attrDct.get('time'))/1000))
        ).filter(
            lambda (eventId, did, newsId, timestamp): \
                    (eventId in VALID_EVENTID_LST) \
                    and (((NOW - timestamp).total_seconds() / (60 * 60.)) <= 24)
        ).map(
            lambda (eventId, did, newsId, timestamp): (newsId,
                calcSco(eventId, timestamp))
        ).reduceByKey(
            lambda x, y: x + y
        ).join(newsMetaRdd, 128).cache()
    redisCli_online = Redis(host='10.8.7.6', port=6379)
    redisCli_sandbox = Redis(host='10.8.16.33', port=6379)
    cleanDirectory()
    for curChannelId, (threCnt, threDay) in CHANNEL_THRESHOLD_DCT.items():
        uvNewsLst = uvRdd.filter(
                lambda (newsId, (cnt, (chId, fetchTime))): \
                        (True if (curChannelId == 10001) else \
                        (int(chId) == curChannelId)) and (cnt >= threCnt)
            ).mapValues(
                lambda (cnt, (chId, fetchTime)): cnt
            ).collect()
        sortedLst = sorted(uvNewsLst, key=lambda val: val[1],
                reverse=True)[:CURRENT_AVAILABLE_CNT]
        if redisCli_online.exists(REDIS_POPULAR_NEWS_PREFIX % curChannelId):
            redisCli_online.delete(REDIS_POPULAR_NEWS_PREFIX % curChannelId)
        if redisCli_sandbox.exists(REDIS_POPULAR_NEWS_PREFIX % curChannelId):
            redisCli_sandbox.delete(REDIS_POPULAR_NEWS_PREFIX % curChannelId)

        fileName = os.path.join(TMP_DIR, '%s_%s_SUCCESS.dat' \
                % (datetime.now().strftime('%Y-%m-%d_%H:%M'), curChannelId))
        with open(fileName, 'w') as fp:
            for newsId, cnt in sortedLst:
                print >>fp, '%s\t%s' % (newsId, int(cnt))
                redisCli_online.rpush(REDIS_POPULAR_NEWS_PREFIX % curChannelId, newsId)
                redisCli_sandbox.rpush(REDIS_POPULAR_NEWS_PREFIX % curChannelId, newsId)

def temporaryChannelPush(newsIdLst, channelId):
    redisCli_online = Redis(host='10.8.7.6', port=6379)
    redisCli_sandbox = Redis(host='10.8.16.33', port=6379)
    for idx, newsId in enumerate(newsIdLst):
        print '%s. insert newsId:%s' % (idx, newsId)
        redisCli_online.lpush(REDIS_POPULAR_NEWS_PREFIX % channelId, newsId)
        redisCli_sandbox.lpush(REDIS_POPULAR_NEWS_PREFIX % channelId, newsId)

if __name__ == '__main__':
    sc = SparkContext(appName='calcUV/limeng@agilanews.com')
    end_date = date.today() + timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    parser = OptionParser()
    parser.add_option('-a', '--action', dest='action')
    (options, args) = parser.parse_args()
    if options.action == 'popular':
        calcNewsUV(sc, start_date, end_date)
    elif options.action == 'temporary':
        TEMP_NEWSID_LST = ['YMIV9IXLfSA=',
                           'fwVmesWbsR8=',
                           'zClqJ2fEyQU=',
                           'zDM7aM182GU=',
                           'UWR9cebekfE=',
                           'lHMo1X0d678=',
                           'E6b+Y1wlqJE=']
        temporaryChannelPush(TEMP_NEWSID_LST, 10001)

