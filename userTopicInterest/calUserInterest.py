# -*- coding:utf-8 -*-
import os
from datetime import date, datetime, timedelta
import json
import urllib
import MySQLdb

from pyspark import SparkContext

TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'

HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ATRICLE_DISPLAY = '020104'
ARTICLE_CLICK = '020103'
ARTICLE_LIKE = '020204'
ARTICLE_COLLECT = '020205'
GRAVITY = 10

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
    newsChDct = getNewsChannel()
    bNewsChannelDct = sc.broadcast(newsChDct)
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
                channelId = bNewsChannelDct.value.get(newsId, None)
                if not channelId:
                    continue
                resLst.append((userId, channelId, newsId,
                    eventId, timestamp))
        elif eventId == ARTICLE_CLICK:
            newsId = attrDct['news_id'].encode('utf-8')
            channelId = bNewsChannelDct.value.get(newsId, None)
            if not channelId:
                return resLst
            resLst.append((userId, channelId, newsId,
                eventId, timestamp))
        return resLst
    logRdd = sc.textFile(','.join(fileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: (attrDct.get('event-id') in eventIdLst) and \
                                ('did' in attrDct) and \
                                ('time' in attrDct)
            ).flatMap(
                # userId, channelId, newsId, eventId, timestamp
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
                lambda (userId, channelId, newsId, eventId, timestamp): \
                        eventId == ATRICLE_DISPLAY
            ).map(
                lambda (userId, channelId, newsId, eventId, timestamp): \
                        ((channelId, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((channelId, weekIdx), newsIdLst): \
                        (weekIdx, (channelId, len(set(newsIdLst))))
            ).groupByKey().mapValues(getProportion)
    categoryLst = categoryRdd.collect()
    categoryDct = {}
    for weekIdx, channelProLst in categoryLst:
        for channelId, proportion in channelProLst:
            categoryDct[(weekIdx, channelId)] = proportion
    return categoryDct

def combineInterest(weekChannelLst, bCategoryDct, latestWeekIdx):
    channelPostDct = {}
    weekCliDct = {}
    # news click for each week, N(t)
    totalCliCnt = 0
    for weekIdx, channelId, newsCnt in weekChannelLst:
        #if weekIdx == latestWeekIdx:
        #    continue
        weekCliDct[weekIdx] = weekCliDct.get(weekIdx, 0) + newsCnt
        totalCliCnt += newsCnt
    # sum of post sum(p(category=c(i)|click) / p(category=c(i)))
    for weekIdx, channelId, newsCnt in weekChannelLst:
        #if weekIdx == latestWeekIdx:
        #    continue
        pCategory = bCategoryDct.value[(weekIdx, channelId)]
        pCategoryCli = newsCnt / weekCliDct[weekIdx]
        score = weekCliDct[weekIdx] * (pCategoryCli / pCategory)
        channelPostDct[channelId] = channelPostDct.get(channelId, 0) + score
    # user's news interests in the near future
    channelPostLst = []
    for channelId, score in channelPostDct.items():
        pCurCategory = bCategoryDct.value[(latestWeekIdx, channelId)]
        score = pCurCategory * (score + GRAVITY) / (totalCliCnt + GRAVITY)
        channelPostLst.append((channelId, score))
    return channelPostLst

def getUserInterest(logRdd, bCategoryDct, latestWeekIdx):
    interestRdd = logRdd.filter(
                lambda (userId, channelId, newsId, eventId, timestamp): \
                        eventId == ARTICLE_CLICK
            ).map(
                lambda (userId, channelId, newsId, eventId, timestamp): \
                        ((userId, channelId, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((userId, channelId, weekIdx), newsIdLst): \
                        (userId, (weekIdx, channelId, len(set(newsIdLst))))
            ).groupByKey(64).mapValues(
                lambda weekChannelLst: combineInterest(weekChannelLst,
                    bCategoryDct, latestWeekIdx)
            )
    return interestRdd

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
    latestWeekIdx = getWeekIndex(end_date - timedelta(days=1))
    getUserInterest(logRdd, bCategoryDct, latestWeekIdx)

