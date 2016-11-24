# -*- coding:utf-8 -*-

import os
import re
from datetime import date, datetime, timedelta
from random import random
import json
from optparse import OptionParser
import MySQLdb
from redis import Redis
import urllib

from pyspark import SparkContext
import settings

KDIR = os.path.dirname(os.path.abspath(__file__))
TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ARTICLE_CLICK_EVENTID = '020103'
ARTICLE_DISPLAY_EVENTID = '020104'
ARTICLE_LIKE_EVENTID = '020204'
ARTICLE_COMMENT_EVENTID = '020207'
EVENTID_LST = [ARTICLE_CLICK_EVENTID,
               ARTICLE_DISPLAY_EVENTID,
               ARTICLE_LIKE_EVENTID,
               ARTICLE_COMMENT_EVENTID]
ROUND_CNT = 5
HOUR = 60 * 60
POSITIVE_TAG = 1
NEGATIVE_TAG = -1
FEATURE_MAP_NAME = 'feature_mapping.listPage'
#DATA_DIR = '/data/models/liblinear'
ALG_NEWS_FEATURE_KEY = 'ALG_NEWS_FEATURE_KEY'
MIN_FEATURE_VALUE = 0.001

def getSpanFileLst(start_date, end_date, withToday=False):
    fileLst = []
    if start_date >= end_date:
        return fileLst
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
        withToday = True
    if withToday:
        res = os.popen('%s fs -ls %s' % \
                (HADOOP_BIN, TODAY_LOG_DIR)).readlines()
        if len(res):
            fileLst.append(TODAY_LOG_DIR)

    # append history's log
    cur_date = start_date
    while cur_date < end_date:
        if cur_date >= date.today():
            cur_date += timedelta(days=1)
            continue
        curDir = os.path.join(HISTORY_LOG_DIR,
                              cur_date.strftime('%Y/%m/%d'),
                              'useraction.log-*')
        res = os.popen("%s fs -ls %s" % (HADOOP_BIN, curDir)).readlines()
        if len(res) != 0:
            fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getOriginalLog(start_date, end_date, sc):
    fileLst = getSpanFileLst(start_date, end_date)
    def _(eventId, did, newsId, newsIdLst, timestamp):
        if eventId in (ARTICLE_DISPLAY_EVENTID, ):
            tmpLst = [(eventId, did, curNewsId, timestamp) \
                    for curNewsId in newsIdLst]
        else:
            tmpLst = [(eventId, did, newsId, timestamp), ]
        return tmpLst
    logRdd = sc.textFile(','.join(fileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: (attrDct.get('event-id') in EVENTID_LST) and \
                                ('did' in attrDct) and \
                                ('time' in attrDct)
            ).map(
                lambda attrDct: (
                    attrDct['event-id'].encode('utf-8'),
                    attrDct['did'].encode('utf-8'),
                    attrDct.get('news_id', u'').encode('utf-8'),
                    [curNewsId.encode('utf-8') for curNewsId in \
                            attrDct.get('news',[]) if type(curNewsId)==unicode],
                    datetime.fromtimestamp(float(attrDct['time'])/1000))
            ).flatMap(
                lambda (eventId, did, newsId, newsIdLst, timestamp): \
                        _(eventId, did, newsId, newsIdLst, timestamp)
            ).cache()
    return logRdd

def aggregateFeatures(actRdd):
    aggFeatureRdd = actRdd.map(
                lambda (eventId, did, newsId, timestamp): \
                        (eventId, did, newsId)
            ).distinct(128).map(
                lambda (eventId, did, newsId): (newsId,
                    (1 if (eventId == ARTICLE_DISPLAY_EVENTID) else 0,
                     1 if (eventId == ARTICLE_CLICK_EVENTID) else 0,
                     1 if (eventId == ARTICLE_LIKE_EVENTID) else 0,
                     1 if (eventId == ARTICLE_COMMENT_EVENTID) else 0))
            ).reduceByKey(
                lambda x, y: (x[0] + y[0],
                              x[1] + y[1],
                              x[2] + y[2],
                              x[3] + y[3]), 128
            ).mapValues(
                lambda (displayCnt, clickCnt, likeCnt, commentCnt): \
                        (max(displayCnt, clickCnt, likeCnt, commentCnt),
                         clickCnt, likeCnt, commentCnt)
            ).filter(
                lambda (newsId, (displayCnt, clickCnt, likeCnt, commentCnt)): \
                        displayCnt > 0
            ).mapValues(
                lambda (displayCnt, clickCnt, likeCnt, commentCnt): \
                        (displayCnt, clickCnt, likeCnt, commentCnt,
                         round(clickCnt/float(displayCnt), ROUND_CNT),
                         round(likeCnt/float(displayCnt), ROUND_CNT),
                         round(commentCnt/float(displayCnt), ROUND_CNT))
            )
    return aggFeatureRdd

def getNewsActionFeatures(logRdd):
    '''
    TODO: cannot be used until realtime feature dumping works
    # get recent action detail
    endTimestamp = datetime.now()
    startTimestamp = endTimestamp - timedelta(days=1)
    recentActRdd = logRdd.filter(
                lambda (eventId, did, newsId, timestamp): \
                        (timestamp >= startTimestamp) and \
                        (timestamp <= endTimestamp)
            )
    recentActFeatureRdd = aggregateFeatures(recentActRdd)
    '''
    # get history action detail
    historyActFeatureRdd = aggregateFeatures(logRdd)
    return historyActFeatureRdd

def getTitleLen(titleStr):
    wordLst = titleStr.strip().split()
    return len(wordLst)

def getContentFeatures(jsonTxt):
    imagePat = re.compile(r'<!--IMG\d+-->')
    imageLst = imagePat.findall(jsonTxt)
    videoPat = re.compile(r'<!--YOUTUBE\d+-->')
    videoLst = videoPat.findall(jsonTxt)
    wordLst = jsonTxt.strip().split()
    return (len(imageLst), len(videoLst), len(wordLst))

def getCursor():
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    mysqlCfg = envCfg.get('mysql_config', {})
    if not mysqlCfg:
        return None
    conn = MySQLdb.connect(host=mysqlCfg['host'],
                           user=mysqlCfg['user'],
                           passwd=urllib.quote(mysqlCfg['passwd']),
                           port=mysqlCfg['port'],
                           db=mysqlCfg['database'])
    conn.autocommit(True)
    cursor = conn.cursor()
    return cursor

def getNewsMetaFeatures(start_date=None, end_date=None):
    cursor = getCursor()
    sqlCmd = '''
select
    url_sign,
    title,
    json_text,
    publish_time,
    fetch_time
from
    tb_news
where
    (
        channel_id not in (10011, 10012, 30001)
    )
    and (is_visible = 1)
    and (
        date(
            from_unixtime(publish_time)
        ) BETWEEN '%s' and '%s'
    )
'''
    if not start_date:
        start_date = date(2015, 1, 1)
    if not end_date:
        end_date = date.today() + timedelta(days=1)
    startDateStr = start_date.strftime('%Y-%m-%d')
    endDateStr = end_date.strftime('%Y-%m-%d')
    cursor.execute(sqlCmd % (startDateStr, endDateStr))
    newsLst = cursor.fetchall()
    newsFeatureDct = {}
    for idx, (newsId, title, jsonTxt, pubTime, fetTime) in \
            enumerate(newsLst):
        if idx % 100 == 0:
            print '%s left...' % (len(newsLst) - idx)
        titleLen = getTitleLen(title)
        (imageCnt, videoCnt, contentLen) = getContentFeatures(jsonTxt)
        pubTime = datetime.fromtimestamp(pubTime)
        fetTime = datetime.fromtimestamp(fetTime)
        newsFeatureDct[newsId] = (titleLen, imageCnt, videoCnt,
                contentLen, pubTime, fetTime)
    return newsFeatureDct

def combineFeatures(logRdd, bMetaFeatureDct, isDaily=False):
    # news action features
    actFeatureRdd = getNewsActionFeatures(logRdd)
    def _(newsId, actFeatureLst, bMetaFeatureDct):
        (displayCnt, clickCnt, likeCnt, commentCnt,
                clickRat, likeRat, commentRat) = actFeatureLst
        if newsId not in bMetaFeatureDct.value:
            return (newsId, None)
        (titleLen, imageCnt, videoCnt, contentLen,
                pubTime, fetTime) = bMetaFeatureDct.value[newsId]
        if isDaily:
            pubTime = int(pubTime.strftime('%s'))
            fetTime = int(fetTime.strftime('%s'))
        featureLst = [displayCnt, clickCnt, likeCnt,
                commentCnt, clickRat, likeRat, commentRat,
                titleLen, imageCnt, videoCnt, contentLen,
                pubTime, fetTime]
        return (newsId, featureLst)
    featureRdd = actFeatureRdd.map(
                lambda (newsId, actFeatureLst): \
                        _(newsId, actFeatureLst, bMetaFeatureDct)
            ).filter(
                lambda (newsId, featureLst): featureLst
            )
    return featureRdd

def getMaxLabelLog(logRdd):
    def _actMerge(preTup, latTup):
        preEventId, preTimestamp = preTup
        latEventId, latTimestamp = latTup
        if preEventId == latEventId:
            eventId = preEventId
        else:
            eventId = ARTICLE_CLICK_EVENTID
        timestamp = max(preTimestamp, latTimestamp)
        return (eventId, timestamp)
    logRdd = logRdd.map(
                lambda (eventId, did, newsId, timestamp): \
                        ((did, newsId), (eventId, timestamp))
            ).reduceByKey(
                lambda preTup, latTup: _actMerge(preTup, latTup)
            ).mapValues(
                lambda (eventId, timestamp): (ARTICLE_DISPLAY_EVENTID \
                        if str(eventId)==ARTICLE_DISPLAY_EVENTID else \
                        ARTICLE_CLICK_EVENTID, timestamp)
            )
    return logRdd

def loagFeatureMap(featureFile):
    if not hasattr(loagFeatureMap, 'featureNameLst'):
        featureNameLst = []
        if not os.path.isfile(featureFile):
            print 'AG feature file not exist!'
            exit(1)
        with open(featureFile, 'r') as fp:
            for line in fp:
                featureNameLst.append(line.strip())
        loagFeatureMap.featureNameLst = featureNameLst
    return loagFeatureMap.featureNameLst

def getSampleLog(maxLabelLogRdd, featureRdd):
    featureMapPath = os.path.join(KDIR, FEATURE_MAP_NAME)
    featureNameLst = loagFeatureMap(featureMapPath)
    def _(did, eventId, timestamp, featureLst):
        (displayCnt, clickCnt, likeCnt, commentCnt,
                clickRat, likeRat, commentRat, titleLen,
                imageCnt, videoCnt, contentLen, pubTime,
                fetTime) = featureLst
        fetTimeInterval = (timestamp - fetTime).total_seconds()
        fetTimeInterval /= HOUR
        if fetTimeInterval <= 0:
            fetTimeInterval = MIN_FEATURE_VALUE
        pubTimeInterval = (timestamp - pubTime).total_seconds()
        pubTimeInterval /= HOUR
        if pubTimeInterval <= 0:
            pubTimeInterval = MIN_FEATURE_VALUE
        featureDct = {'HISTORY_DISPLAY_COUNT': displayCnt,
                      'HISTORY_READ_COUNT': clickCnt,
                      'HISTORY_LIKE_COUNT': likeCnt,
                      'HISTORY_COMMENT_COUNT': commentCnt,
                      'HISTORY_READ_DISPLAY_RATIO': clickRat,
                      'HISTORY_LIKE_DISPLAY_RATIO': likeRat,
                      'HISTORY_COMMENT_DISPLAY_RATIO': commentRat,
                      'PICTURE_COUNT': imageCnt,
                      'VIDEO_COUNT': videoCnt,
                      'TITLE_LENGTH': titleLen,
                      'CONTENT_LENGTH': contentLen,
                      'FETCH_TIMESTAMP_INTERVAL': fetTimeInterval,
                      'POST_TIMESTAMP_INTERTVAL': pubTimeInterval}
        if eventId == ARTICLE_DISPLAY_EVENTID:
            label = NEGATIVE_TAG
        else:
            label = POSITIVE_TAG
        valueLst = [str(label), ]
        for idx, curFeatureName in enumerate(featureNameLst):
            value = featureDct.get(curFeatureName, 0)
            if value:
                valueLst.append('%s:%s' % (idx+1, value))
        return valueLst
    sampleRdd = maxLabelLogRdd.join(featureRdd, 128).mapValues(
                lambda ((did, eventId, timestamp), featureLst): \
                        _(did, eventId, timestamp, featureLst)
            ).map(
                lambda (newsId, valsLst): valsLst
            )
    return sampleRdd

def sample(sampleRdd, posRatio, negRatio):
    def _(valsLst):
        tag = int(valsLst[0])
        if tag == POSITIVE_TAG:
            if random() <= posRatio:
                return valsLst
            else:
                return None
        elif tag == NEGATIVE_TAG:
            if random() <= negRatio:
                return valsLst
            else:
                return None
    sampleRdd = sampleRdd.map(
                lambda valsLst: _(valsLst)
            ).filter(
                lambda valsLst: valsLst
            ).map(
                lambda valsLst: ' '.join(valsLst)
            )
    sampleLst = sampleRdd.collect()
    with open(SAMPLE_FILENAME, 'w') as fp:
        for line in sampleLst:
            print >>fp, line

if __name__ == '__main__':
    sc = SparkContext(appName='rerank/limeng@agilanews.com',
            pyFiles=[])
    parser = OptionParser()
    parser.add_option('-s', '--start', dest='start_date')
    parser.add_option('-e', '--end', dest='end_date')
    parser.add_option('-a', '--action', dest='action')
    parser.add_option('--clickRatio', dest='clickRatio', default=1.)
    parser.add_option('--displayRatio', dest='displayRatio', default=1.)
    parser.add_option('--dataDir', dest="dataDir", default='')
    (options, args) = parser.parse_args()

    start_date = datetime.strptime(options.start_date, '%Y%m%d').date()
    end_date = datetime.strptime(options.end_date, '%Y%m%d').date()
    logRdd = getOriginalLog(start_date, end_date, sc)
    global DATA_DIR
    global SAMPLE_FILENAME
    if not options.dataDir:
        print 'data directory missing, error!'
        exit(0)
    DATA_DIR = options.dataDir
    SAMPLE_FILENAME = os.path.join(DATA_DIR, 'sample.dat')
    maxLabelLogRdd = getMaxLabelLog(logRdd)
    if options.action == 'verbose':
        categoryRdd = maxLabelLogRdd.map(
                    lambda ((did, newsId), (eventId, timestamp)): \
                            (eventId, 1)
                ).reduceByKey(
                    lambda x, y: x + y, 64
                )
        categoryLst = categoryRdd.collect()
        print '=' * 60
        for idx, (category, cnt) in enumerate(categoryLst):
            print '%s. eventId:%s, cnt:%s' % (idx, category, cnt)
        print '=' * 60
    elif options.action == 'sample':
        # news meta features
        news_start_date = start_date - timedelta(days=10)
        metaFeatureDct = getNewsMetaFeatures(
                start_date=news_start_date, end_date=end_date)
        bMetaFeatureDct = sc.broadcast(metaFeatureDct)
        featureRdd = combineFeatures(logRdd, bMetaFeatureDct)
        maxLabelLogRdd = maxLabelLogRdd.map(
                lambda ((did, newsId), (eventId, timestamp)): \
                        (newsId, (did, eventId, timestamp))
            )
        sampleRdd = getSampleLog(maxLabelLogRdd, featureRdd)
        clickRatio = float(options.clickRatio)
        displayRatio = float(options.displayRatio)
        sample(sampleRdd, clickRatio, displayRatio)
    elif options.action == 'daily':
        end_date = date.today() + timedelta(days=1)
        start_date = end_date - timedelta(days=5)
        metaFeatureDct = getNewsMetaFeatures(
                start_date=start_date, end_date=end_date)
        bMetaFeatureDct = sc.broadcast(metaFeatureDct)
        featureRdd = combineFeatures(logRdd, bMetaFeatureDct,
                isDaily=True)
        newsFeatureLst = featureRdd.collect()
        env = settings.CURRENT_ENVIRONMENT_TAG
        envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
        redisCfg = envCfg.get('news_queue_redis_config', {})
        if not redisCfg:
            print 'redis configuration not exist!'
            exit(1)
        redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])
        if redisCli.exists(ALG_NEWS_FEATURE_KEY):
            redisCli.delete(ALG_NEWS_FEATURE_KEY)
        totalCnt = 0
        tmpDct = {}
        for idx, (newsId, featureLst) in enumerate(newsFeatureLst):
            totalCnt += 1
            if len(tmpDct) >= 20:
                print '%s remaing...' % (len(newsFeatureLst) - totalCnt)
                redisCli.hmset(ALG_NEWS_FEATURE_KEY, tmpDct)
                tmpDct = {}
            tmpDct[newsId] = json.dumps(featureLst)
        if len(tmpDct):
            redisCli.hmset(ALG_NEWS_FEATURE_KEY, tmpDct)
