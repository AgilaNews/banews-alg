# -*- coding:utf-8 -*-
import os
import json
from pyspark import SparkContext
from datetime import date, datetime, timedelta

TODAY_LOG_DIR = '/banews/useraction.log-%s_*'
HISTORY_LOG_DIR = '/banews/useraction'

AVAILABLE_STRATEGY_LST = ['10001_popularRanking',
                          '10001_personalTopicRec',
                          '10001_lrRanker']
AVAILABLE_CHANNEL_LST = ['10001', ]
LIST_REQUEST_CODE = '020104'
LIST_ARTICLE_REQUEST_CODE = '020103'
AVAILABLE_EVENTID_LST = [LIST_REQUEST_CODE, LIST_ARTICLE_REQUEST_CODE]
SERVER_FRESH_CODE = '040101'
CLIENT_FRESH_CODE = '040102'

def getSpanFileLst(start_date, end_date, withToday=False):
    fileLst = []
    if start_date >= end_date:
        return fileLst
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
        withToday = True
    if withToday:
        fileLst.append(TODAY_LOG_DIR % date.today().strftime('%Y-%m-%d'))

    # append history's log
    cur_date = start_date
    while cur_date < end_date:
        if cur_date >= date.today():
            cur_date += timedelta(days=1)
            continue
        curDir = os.path.join(HISTORY_LOG_DIR, \
                cur_date.strftime('%Y/%m/%d'), 'useraction.log-*')
        fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getTransferTime(timestamp):
    if timestamp:
        timestamp = datetime.fromtimestamp(float(timestamp)/1000.).date()
        return timestamp
    else:
        return None

def calcCliDisRatio(sc, start_date, end_date):
    fileLst = getSpanFileLst(start_date, end_date)
    originalRdd = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: json.loads(dctStr)
        ).filter(
            lambda attrDct: attrDct.get('client-version') != '1.2.5'
        ).map(
            lambda attrDct: (attrDct.get('event-id'),
                             attrDct.get('did'),
                             attrDct.get('news_id'),
                             attrDct.get('news'),
                             attrDct.get('session'),
                             attrDct.get('policy'),
                             attrDct.get('channel_id'),
                             getTransferTime(attrDct.get('time')))
        ).filter(
            lambda (eventId, deviceId, newsId, newsIdLst, sessionId,
                strategy, channelId, timestamp): \
                (eventId in AVAILABLE_EVENTID_LST) and \
                (timestamp >= start_date) and \
                (timestamp < end_date) and \
                sessionId
        ).cache()

    clickRdd = originalRdd.filter(
            lambda (eventId, deviceId, newsId, newsIdLst, sessionId,
                strategy, channelId, timestamp): \
                (eventId == LIST_ARTICLE_REQUEST_CODE) and newsId
        ).map(
            lambda (eventId, deviceId, newsId, newsIdLst, sessionId,
                strategy, channelId, timestamp): \
                ((timestamp, sessionId, newsId, deviceId), 1)
        ).distinct()
    listRdd = originalRdd.filter(
            lambda (eventId, deviceId, newsId, newsIdLst, sessionId,
                strategy, channelId, timestamp): \
                (eventId == LIST_REQUEST_CODE) and \
                (channelId in AVAILABLE_CHANNEL_LST) and \
                newsIdLst
        ).flatMap(
            lambda (eventId, deviceId, newsId, newsIdLst, sessionId,
                strategy, channelId, timestamp): \
                [((timestamp, sessionId, curNewsId, deviceId), strategy) \
                for curNewsId in newsIdLst]
        ).distinct()

    combineRdd = listRdd.leftOuterJoin(clickRdd, 128).map(
            lambda ((curDate, sessionId, newsId, deviceId),
                (strategy, flag)): ((curDate, strategy),
                (set([deviceId, ]), 1, 1 if flag else 0))
        ).reduceByKey(
            lambda x, y: (x[0] | y[0], x[1] + y[1], x[2] + y[2])
        ).mapValues(
            lambda (deviceIdSet, requestCnt, clickCnt): \
                    (len(deviceIdSet), requestCnt, clickCnt)
        )
    resLst = combineRdd.collect()
    print '=' * 100
    sortedLst = sorted(resLst, key=lambda val:val[0][0], reverse=True)
    for ((time, strategy), (userCnt, totDisCnt, totCliCnt)) in sortedLst:
        print 'date:%s, strategy:%s, userCnt:%s, total display cnt:%s, ' \
                'total click cnt:%s, ratio:%.3f' % (time, strategy, userCnt,
                totDisCnt, totCliCnt, float(totCliCnt)/totDisCnt)
    print '=' * 100

if __name__ == '__main__':
    sc = SparkContext(appName='strategyAnalysis/limeng@agilanews.com')
    end_date = date.today() + timedelta(days=1)
    start_date = end_date - timedelta(days=2)
    calcCliDisRatio(sc, start_date, end_date)
