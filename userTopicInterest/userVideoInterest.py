#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# @Date    : 2017-02-21 11:39:43
# @Author  : Zhao Yulong (elysium.zyl@gmail.com)
# @Link    : ${link}
# @Describe: 
"""
import os
import traceback

import argparse
from datetime import date, datetime, timedelta
import json
import urllib
import MySQLdb
from redis import Redis
try:
    from pyspark import SparkContext
except:
    pass

import settings


TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'

HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'

VIDEO_DISPATCH_ACTION = set(['020104'])
VIDEO_PLAY_ACTION = set(['020301','020102'])

GRAVITY = 10.
MIN_USER_ACTION_CNT = 10

ALG_USER_YOUTUBE_CHANNEL_KEY = 'ALG_USER_YOUTUBE_CHANNEL_KEY'
ALG_YOUTUBE_CHANNEL_GRAVITY_KEY = 'ALG_YOUTUBE_CHANNEL_GRAVITY_KEY'


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


def line_tran(line):
    try:
        return json.loads(line)
    except:
        return {}

def isTargetChannel(channel_id):
    if channel_id == "30001":
        return True

def getTransferTime(timestamp):
    if timestamp:
        tm = datetime.fromtimestamp(float(timestamp)/1000.)
        tm = datetime(tm.year, tm.month, tm.day)
        return tm
    else:
        return None

def action_filter(line, start, end):
    tm = getTransferTime(line.get("time"))
    if not tm:
        return False
    tm = tm.date()
    did = line.get("did", None)
    if not did:
        return False
    if tm < start or tm >= end:
        return False
    event_id = line.get("event-id", None)
    if event_id in VIDEO_DISPATCH_ACTION:
        channel_id = line.get("channel_id")
        if isTargetChannel(channel_id):
            return True
    elif event_id in VIDEO_PLAY_ACTION:
        channel_id = line.get("refer", None)
        if not channel_id:
            return True
        elif isTargetChannel(channel_id):
            return True
        else:
            return False
    return False


def line_mapper(attrDct, bcNewsChannelDct):
    resLst = []

    userId = attrDct['did'].encode('utf-8')
    eventId = attrDct['event-id'].encode('utf-8')
    timestamp = datetime.fromtimestamp(\
            float(attrDct['time'])/1000.).date()
    if eventId in VIDEO_DISPATCH_ACTION:
        if ('news' not in attrDct) or (not attrDct['news']):
            return resLst
        for newsId in attrDct['news']:
            if (type(newsId) == int) or (not newsId):
                continue
            newsId = newsId.encode('utf-8')
            youtube_channel_id = bcNewsChannelDct.value.get(newsId, None)
            if not youtube_channel_id:
                continue
            resLst.append((userId, youtube_channel_id, newsId, eventId,
                timestamp))
    elif eventId in VIDEO_PLAY_ACTION:
        newsId = attrDct.get("news_id", None)
        if not newsId:
            return resLst
        newsId = newsId.encode('utf-8')
        youtube_channel_id = bcNewsChannelDct.value.get(newsId, None)
        if youtube_channel_id:
            resLst.append((userId, youtube_channel_id, newsId, eventId,
            timestamp))
    return resLst


def getNewsChannels():
    db = MySQLdb.connect("10.8.22.123", "banews_w", "MhxzKhl-Happy", "banews")
    sql = '''select news_url_sign, youtube_channel_id from `banews`.`tb_video`'''
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    videoChannelMap = {}
    for result in results:
        videoChannelMap[result[0]] = result[1]
    return videoChannelMap


def getActionLogRdd(sc, start_date, end_date):
    fileLst = getSpanFileLst(start_date, end_date)
    newsChannelDct = getNewsChannels()
    bcNewsChannelDct = sc.broadcast(newsChannelDct)

    logRdd = sc.textFile(','.join(fileLst)).map(
                lambda dctStr: json.loads(dctStr)
            ).filter(
                lambda line: action_filter(line, start_date, end_date)
            ).flatMap(
                lambda line: line_mapper(line, bcNewsChannelDct)
            ).cache()
    return logRdd


def getWeekIndex(timestamp):
    if type(timestamp) not in (date, datetime):
        return -1
    else:
        (year, weekIdx, weekDay) = timestamp.isocalendar()
        return year * 100 + weekIdx


def getProportion(valLst):
    totalVal = float(sum(map(lambda val: val[1], valLst)))
    resLst = []
    for key, val in valLst:
        resLst.append((key, val/totalVal))
    return resLst


def getCategoryWeek(logRdd):
    categoryRdd = logRdd.filter(
                lambda (userId, youtube_channel_id, newsId, eventId, timestamp): \
                        eventId in VIDEO_DISPATCH_ACTION
            ).map(
                lambda (userId, youtube_channel_id, newsId, eventId, timestamp): \
                        ((youtube_channel_id, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((youtube_channel_id, weekIdx), newsIdLst): \
                        (weekIdx, (youtube_channel_id, len(set(newsIdLst))))
            ).groupByKey().mapValues(getProportion)
    categoryLst = categoryRdd.collect()
    categoryDct = {}
    for weekIdx, topicProLst in categoryLst:
        for youtube_channel_id, proportion in topicProLst:
            categoryDct[(weekIdx, youtube_channel_id)] = proportion
    return categoryDct


def combineInterest(weekChannelLst, bCategoryDct):
    channelPostDct = {}
    weekCliDct = {}
    # news click for each week, N(t)
    totalCliCnt = 0.
    for weekIdx, youtube_channel_id, newsCnt in weekChannelLst:
        weekCliDct[weekIdx] = float(weekCliDct.get(weekIdx, 0)) + newsCnt
        totalCliCnt += newsCnt
    # sum of post sum(p(category=c(i)|click) / p(category=c(i)))
    for weekIdx, youtube_channel_id, newsCnt in weekChannelLst:
        try:
            #print "has key:", weekIdx, youtube_channel_id
            pCategory = bCategoryDct.value[(weekIdx, youtube_channel_id)]
            pCategoryCli = newsCnt / weekCliDct[weekIdx]
            score = weekCliDct[weekIdx] * (pCategoryCli / pCategory)
            channelPostDct[youtube_channel_id] = channelPostDct.get(youtube_channel_id, 0) + score
        except:
            print "no key:", weekIdx, youtube_channel_id
    # user's news interests in the near future
    channelPostLst = []
    for youtube_channel_id, score in channelPostDct.items():
        score = (score + GRAVITY) / (totalCliCnt + GRAVITY)
        channelPostLst.append((youtube_channel_id, score))
    # normalizeing user's news interests
    newChannelPostLst = []
    totalSco = float(sum(map(lambda val: val[1], channelPostLst)))
    sortedLst = sorted(channelPostLst, key=lambda val: val[1],
            reverse=True)
    for youtube_channel_id, score in sortedLst:
        newChannelPostLst.append((youtube_channel_id, score/totalSco))
    return (newChannelPostLst, totalCliCnt)


def getUserInterestRdd(logRdd, bCategoryDct):
    interestRdd = logRdd.filter(
                lambda (userId, youtube_channel_id, newsId, eventId, timestamp): \
                        eventId in VIDEO_PLAY_ACTION
            ).map(
                lambda (userId, youtube_channel_id, newsId, eventId, timestamp): \
                        ((userId, youtube_channel_id, getWeekIndex(timestamp)), newsId)
            ).groupByKey(64).map(
                lambda ((userId, youtube_channel_id, weekIdx), newsIdLst): \
                        (userId, (weekIdx, youtube_channel_id, len(set(newsIdLst))))
            ).groupByKey(64).mapValues(
                lambda weekChannelLst: combineInterest(weekChannelLst,
                    bCategoryDct)
            ).filter(
                lambda (userId, (newsChannelPostLst, totalCliCnt)): \
                        newsChannelPostLst and \
                        (totalCliCnt > MIN_USER_ACTION_CNT)
            )
    return interestRdd


def dump(userInterestLst, env):
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        return None
    # dumping to redis

    print 'dump %s users topic interests...' % len(userInterestLst)
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])
    tmpDct = {}
    totalCnt = 0
    if redisCli.exists(ALG_USER_YOUTUBE_CHANNEL_KEY):
        redisCli.delete(ALG_USER_YOUTUBE_CHANNEL_KEY)
    for userId, (channelPostLst, totalCliCnt) in userInterestLst:
        totalCnt += 1
        if len(tmpDct) >= 20:
            print '%s remain....' % (len(userInterestLst) - totalCnt)
            redisCli.hmset(ALG_USER_YOUTUBE_CHANNEL_KEY, tmpDct)
            tmpDct = {}
        tmpDct[userId] = json.dumps((channelPostLst, totalCliCnt))
    if len(tmpDct):
        redisCli.hmset(ALG_USER_YOUTUBE_CHANNEL_KEY, tmpDct)
    redisCli.set(ALG_YOUTUBE_CHANNEL_GRAVITY_KEY, GRAVITY)


def main(start_date, end_date):
    print "start calculate ..."
    sc = SparkContext(appName='videoTrend/zhaoyulong')

    print "get logrdd"
    logRdd = getActionLogRdd(sc, start_date, end_date)

    # category distribution each week
    print "get category week"
    categoryDct = getCategoryWeek(logRdd)
    bCategoryDct = sc.broadcast(categoryDct)
    # combine user's short-term & long-term interest
    print "get interest rdd"
    interestRdd = getUserInterestRdd(logRdd, bCategoryDct)
    dump(interestRdd.collect(), 'sandbox')
    dump(interestRdd.collect(), 'online')
    print "end"


def test(start_date, end_date):
    return


def formatDate(datestr):
    return (datetime.strptime(datestr, '%Y%m%d').date())


def getParam():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--action', help="action type",
        default="user_interest", choices=["debug", "user_interest"])
    parser.add_argument('-s', '--start_date', help="start date",
        required=True)
    parser.add_argument('-e', '--end_date', help="end date",
        required=True)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = getParam()
    start_date = formatDate(args.start_date)
    end_date = formatDate(args.end_date)

    if args.action == "debug":
        test(start_date, end_date)
    elif args.action == "user_interest":
        print args.action
        main(start_date, end_date)
