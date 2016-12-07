# -*- coding:utf-8 -*-
import os
import json
from pyspark import SparkContext
from datetime import date, datetime, timedelta
import MySQLdb
import urllib
from redis import Redis

import settings
from trainTopicModel import (getSpanNews,
                             predict)
from calUserInterest import getTopTopics

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
CURRENT_AVAILABLE_CNT = 100
CHANNEL_THRESHOLD_DCT = {
        # channelId, (cntThr, dayThr)
        10001: (10, 1),   # hot
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
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ALG_TOPIC_NEWS_SCO_KEY = 'ALG_TOPIC_NEWS_SCO_KEY'
ALG_TOPIC_RATIO_KEY = 'ALG_TOPIC_RATIO_KEY'

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

def calcSco(eventId, fetchTimeStamp):
    span = (NOW - fetchTimeStamp).total_seconds() / (60 * 60.)
    if eventId in (ARTICLE_LIKE, ARTICLE_COMMENT):
        sco = 3.
    else:
        sco = 1.
    sco *= pow(0.5, span / 8)
    return sco

def calcNewsScore(sc, start_date, end_date, dTopTopicDct):
    fileLst = getSpanRdd(start_date, end_date)
    logRdd = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: json.loads(dctStr)
        ).map(
            lambda attrDct: (attrDct.get('event-id'),
                             attrDct.get('did'),
                             attrDct.get('news_id'),
                             getTransferTime(float(attrDct.get('time'))/1000))
        ).filter(
            lambda (eventId, did, newsId, timestamp): \
                    timestamp and (eventId in VALID_EVENTID_LST) \
                    and (((NOW - timestamp).total_seconds() / (60 * 60.)) <= 24)
        ).cache()
    # score foreach news
    scoRdd = logRdd.map(
            lambda (eventId, did, newsId, timestamp): (newsId,
                calcSco(eventId, timestamp))
        ).reduceByKey(
            lambda x, y: x + y
        ).filter(
            lambda (newsId, sco): newsId in dTopTopicDct.value
        ).cache()
    newsScoLst = scoRdd.collect()
    # topic distribution recently
    topicRdd = logRdd.map(
            lambda (eventId, did, newsId, timestamp): \
                    (newsId, set([did]))
        ).reduceByKey(
            lambda x, y: x | y, 64
        ).flatMap(
            lambda (newsId, userIdSet): [(topicIdx, len(userIdSet)) \
                    for topicIdx in dTopTopicDct.value.get(newsId, [])]
        ).reduceByKey(
            lambda x, y: x + y
        )
    recentTopicLst = topicRdd.collect()
    return (newsScoLst, recentTopicLst)

def dump(scoLst, topicLst, env):
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        return None
    redisCli = Redis(host=redisCfg['host'],
                     port=redisCfg['port'])
    # dump news score recently
    if redisCli.exists(ALG_TOPIC_NEWS_SCO_KEY):
        redisCli.delete(ALG_TOPIC_NEWS_SCO_KEY)
    tmpDct= {}
    for idx, (newsId, sco) in enumerate(scoLst):
        if len(tmpDct) >= 20:
            redisCli.hmset(ALG_TOPIC_NEWS_SCO_KEY,
                           tmpDct)
            tmpDct = {}
        tmpDct[newsId] = sco
    if len(tmpDct):
        redisCli.hmset(ALG_TOPIC_NEWS_SCO_KEY,
                       tmpDct)
    # dump topic distribution recently
    if redisCli.exists(ALG_TOPIC_RATIO_KEY):
        redisCli.delete(ALG_TOPIC_RATIO_KEY)
    totalNewsCnt = float(sum(map(lambda val: val[1],
        topicLst)))
    for topicIdx, cnt in topicLst:
        redisCli.hset(ALG_TOPIC_RATIO_KEY, topicIdx,
                cnt/totalNewsCnt)

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=2)
    newsDocLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    newsTopicLst = predict(newsDocLst)
    topTopicDct = {}
    for newsId, topicArr in newsTopicLst:
        topTopicDct[newsId] = getTopTopics(topicArr)
    sc = SparkContext(appName='userTopicDis/limeng@agilanews.com')
    dTopTopicDct = sc.broadcast(topTopicDct)
    end_date = date.today() + timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    (newsScoLst, recentTopicLst) = calcNewsScore(sc,
            start_date, end_date, dTopTopicDct)
    # dump model to sandbox & online
    dump(newsScoLst, recentTopicLst, 'sandbox')
    dump(newsScoLst, recentTopicLst, 'online')
