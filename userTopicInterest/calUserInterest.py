# -*- coding:utf-8 -*-
import os
from datetime import date, datetime, timedelta
import json
import urllib
import MySQLdb
from redis import Redis
from pyspark import SparkContext

import settings

TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'

HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ATRICLE_DISPLAY = '020104'
ARTICLE_CLICK = '020103'
ARTICLE_LIKE = '020204'
ARTICLE_COLLECT = '020205'
GRAVITY = 10
NEWS_TOPICS_PATH = '/data/userTopicDis/model/newsTopic.d'
MIN_NEWS_TOPIC_WEIGHT = 0.1
MIN_USER_ACTION_CNT = 5
USER_TOPIC_INTEREST_DIR = '/user/limeng/userTopicInterest'
ALG_USER_TOPIC_KEY = 'ALG_USER_TOPIC_KEY'

def getNewsChannel(start_date=None, end_date=None):
    conn = MySQLdb.connect(host='10.8.22.123',
                           user='banews_w',
                           passwd=urllib.quote('MhxzKhl-Happy'),
                           port=3306,
                           db='banews')
    conn.autocommit(True)
    cursor = conn.cursor()
    sqlCmd = '''
select
    url_sign,
    channel_id
from
    tb_news
where
    (
        channel_id not in (10011, 10012)
    )
    and (is_visible = 1)
    and (
        date(
            from_unixtime(fetch_time)
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
    newsChDct = dict(cursor.fetchall())
    return newsChDct

def getTopTopics(topicScoLst):
    totalSco = float(sum(topicScoLst))
    if not totalSco:
        return None
    topicIdxLst = []
    maxTopicIdx, maxTopicSco = 0, 0
    for topicIdx, topicSco in enumerate(topicScoLst):
        topicSco = topicSco / totalSco
        if topicSco >= MIN_NEWS_TOPIC_WEIGHT:
            topicIdxLst.append(topicIdx)
        if maxTopicSco < topicSco:
            maxTopicSco = topicSco
            maxTopicIdx = topicIdx
    if not topicIdxLst:
        topicIdxLst.append(maxTopicIdx)
    return topicIdxLst

def getNewsTopics():
    newsTopicDct = {}
    with open(NEWS_TOPICS_PATH, 'r') as fp:
        for line in fp:
            vals = line.strip().split('\t', 1)
            if len(vals) != 2:
                continue
            (newsId, topicStr) = vals
            topicScoLst = [float(val) for val in \
                    topicStr.strip().split(',')]
            newsTopicDct[newsId] = getTopTopics(topicScoLst)
    return newsTopicDct

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

def getActionLog(sc, start_date, end_date):
    # Notice: news only belong to one channel,
    # but can exist in mulitple topics
    newsTopicDct = getNewsTopics()
    bNewsTopicDct = sc.broadcast(newsTopicDct)
    eventIdLst = [ARTICLE_CLICK, ATRICLE_DISPLAY]
    fileLst = getSpanFileLst(start_date, end_date)
    def _(attrDct):
        resLst = []
        userId = attrDct['did'].encode('utf-8')
        eventId = attrDct['event-id'].encode('utf-8')
        timestamp = datetime.fromtimestamp(\
                float(attrDct['time'])/1000.).date()
        if eventId == ATRICLE_DISPLAY:
            for newsId in attrDct['news']:
                if type(newsId) == int:
                    continue
                newsId = newsId.encode('utf-8')
                topicIdxLst = bNewsTopicDct.value.get(newsId, None)
                if not topicIdxLst:
                    continue
                for topicIdx in topicIdxLst:
                    resLst.append((userId, topicIdx, newsId,
                        eventId, timestamp))
        elif eventId == ARTICLE_CLICK:
            newsId = attrDct['news_id'].encode('utf-8')
            topicIdxLst = bNewsTopicDct.value.get(newsId, None)
            if not topicIdxLst:
                return resLst
            for topicIdx in topicIdxLst:
                resLst.append((userId, topicIdx, newsId,
                    eventId, timestamp))
        return resLst
    logRdd = sc.textFile(','.join(fileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: (attrDct.get('event-id') in eventIdLst) and \
                                ('did' in attrDct) and \
                                ('time' in attrDct)
            ).flatMap(
                # userId, topicIdx, newsId, eventId, timestamp
                lambda attrDct: _(attrDct)
            )
    return logRdd

def getWeekIndex(timestamp):
    if type(timestamp) not in (date, datetime):
        return -1
    else:
        (year, weekIdx, weekDay) = timestamp.isocalendar()
        return year * 100 + weekIdx

def getProportion(valLst):
    totalVal = sum(map(lambda val: val[1], valLst))
    resLst = []
    for key, val in valLst:
        resLst.append((key, float(val)/totalVal))
    return resLst

def getCategoryWeek(logRdd):
    categoryRdd = logRdd.filter(
                lambda (userId, topicIdx, newsId, eventId, timestamp): \
                        eventId == ATRICLE_DISPLAY
            ).map(
                lambda (userId, topicIdx, newsId, eventId, timestamp): \
                        ((topicIdx, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((topicIdx, weekIdx), newsIdLst): \
                        (weekIdx, (topicIdx, len(set(newsIdLst))))
            ).groupByKey().mapValues(getProportion)
    categoryLst = categoryRdd.collect()
    categoryDct = {}
    for weekIdx, topicProLst in categoryLst:
        for topicIdx, proportion in topicProLst:
            categoryDct[(weekIdx, topicIdx)] = proportion
    return categoryDct

def combineInterest(weekChannelLst, bCategoryDct):
    channelPostDct = {}
    weekCliDct = {}
    # news click for each week, N(t)
    totalCliCnt = 0.
    for weekIdx, topicIdx, newsCnt in weekChannelLst:
        weekCliDct[weekIdx] = float(weekCliDct.get(weekIdx, 0)) + newsCnt
        totalCliCnt += newsCnt
    # sum of post sum(p(category=c(i)|click) / p(category=c(i)))
    for weekIdx, topicIdx, newsCnt in weekChannelLst:
        pCategory = bCategoryDct.value[(weekIdx, topicIdx)]
        pCategoryCli = newsCnt / weekCliDct[weekIdx]
        score = weekCliDct[weekIdx] * (pCategoryCli / pCategory)
        channelPostDct[topicIdx] = channelPostDct.get(topicIdx, 0) + score
    # user's news interests in the near future
    channelPostLst = []
    for topicIdx, score in channelPostDct.items():
        score = (score + GRAVITY) / (totalCliCnt + GRAVITY)
        channelPostLst.append((topicIdx, score))
    # normalizeing user's news interests
    newChannelPostLst = []
    totalSco = float(sum(map(lambda val: val[1], channelPostLst)))
    sortedLst = sorted(channelPostLst, key=lambda val: val[1],
            reverse=True)
    for topicIdx, score in sortedLst:
        newChannelPostLst.append((topicIdx, score/totalSco))
    return (newChannelPostLst, totalCliCnt)

def getUserInterest(logRdd, bCategoryDct):
    interestRdd = logRdd.filter(
                lambda (userId, topicIdx, newsId, eventId, timestamp): \
                        eventId == ARTICLE_CLICK
            ).map(
                lambda (userId, topicIdx, newsId, eventId, timestamp): \
                        ((userId, topicIdx, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((userId, topicIdx, weekIdx), newsIdLst): \
                        (userId, (weekIdx, topicIdx, len(set(newsIdLst))))
            ).groupByKey(64).mapValues(
                lambda weekChannelLst: combineInterest(weekChannelLst,
                    bCategoryDct)
            ).filter(
                lambda (userId, (newsChannelPostLst, totalCliCnt)): \
                        newsChannelPostLst and \
                        (totalCliCnt > MIN_USER_ACTION_CNT)
            )
    return interestRdd

def dump(interestRdd):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        return None
    # dumping to redis
    userInterestLst = interestRdd.collect()
    print 'dump %s users topic interests...' % len(userInterestLst)
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])
    tmpDct = {}
    totalCnt = 0
    for userId, (channelPostLst, totalCliCnt) in userInterestLst:
        totalCnt += 1
        if len(tmpDct) >= 20:
            print '%s remain....' % (len(userInterestLst) - totalCnt)
            redisCli.hmset(ALG_USER_TOPIC_KEY, tmpDct)
            tmpDct = {}
        tmpDct[userId] = json.dumps((channelPostLst, totalCliCnt))
    if len(tmpDct):
        redisCli.hmset(ALG_USER_TOPIC_KEY, tmpDct)

if __name__ == '__main__':
    sc = SparkContext(appName='newsTrend/limeng')
    end_date = date.today()
    start_date = end_date - timedelta(days=3)
    logRdd = getActionLog(sc, start_date, end_date)
    logRdd = logRdd.cache()
    # category distribution each week
    categoryDct = getCategoryWeek(logRdd)
    bCategoryDct = sc.broadcast(categoryDct)
    # combine user's short-term & long-term interest
    interestRdd = getUserInterest(logRdd, bCategoryDct)
    dump(interestRdd)

