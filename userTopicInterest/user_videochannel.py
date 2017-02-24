#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
# @Date    : 2017-02-17 18:10:48
# @Author  : Zhao Yulong (elysium.zyl@gmail.com)
# @Link    : ${link}
# @Describe: 
"""

import os
import traceback
from redis import Redis
import json
import MySQLdb
import time
from operator import add
from datetime import date, datetime, timedelta
from pyspark import SparkContext
from numpy import mean, median
from urllib import quote, unquote
from operator import itemgetter


TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
LOG_DIR = '/banews/useraction'
LG_YOUTUBE_CHANNEL_RATIO_KEY='ALG_YOUTUBE_CHANNEL_RATIO_KEY'

VIDEO_PLAY_ACTION = set(['020301','020102'])
videoChannelMap = {}

sc = SparkContext(appName='hotvideo/zhaoyulong')


def getFileList(start_date, end_date):
    fileLst = []
    if start_date >= end_date:
        return fileLst
    res = []
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
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


def line_tran(line):
    try:
        return json.loads(line)
    except:
        return {}


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
    if event_id in VIDEO_PLAY_ACTION:
        channel_id = line.get("refer")
        if not channel_id :
            return True
        elif channel_id > "30000" and channel_id < "32000":
            return True
        else:
            return False
    return False


def getTransferTime(timestamp):
    if timestamp:
        tm = datetime.fromtimestamp(float(timestamp)/1000.)
        tm = datetime(tm.year, tm.month, tm.day)
        return tm
    else:
        return None


def line_mapper(line):
    return (
            line.get('event-id'),
            line.get('did'),
            line.get('news_id'),
            line.get('session'),
            getTransferTime(line.get('time'))
            )


def getClickRDD(originalRDD):
    clickRDD = originalRDD.filter(
            lambda (eventId, did, newsId, session, time): \
                    eventId in VIDEO_PLAY_ACTION
        ).map(
            lambda (eventId, did, newsId, session, time): \
                    (did, set([str(newsId), ]))
        ).reduceByKey(
            lambda x, y: x | y
        )
    return clickRDD

def getVideoChannelMap():
    global videoChannelMap
    db = MySQLdb.connect("10.8.22.123", "banews_w", "MhxzKhl-Happy", "banews")
    sql = '''select news_url_sign, youtube_channel_id from `banews`.`tb_video`'''
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        videoChannelMap[result[0]] = result[1]

def videoMapper(line):
    global videoChannelMap
    newsSet = line[1]
    for news in newsSet:
        try:
            yield(videoChannelMap[news], 1)
        except Exception as e:
            pass

def video_stat(fileLst, start, end):
    allfile = ','.join(fileLst)
    originalRDD = sc.textFile(allfile).map(
            lambda dctStr: line_tran(dctStr)
        ).filter(
            lambda line: action_filter(line, start, end)
        ).map(
            lambda line: line_mapper(line)
        ).cache()
    clickRDD = getClickRDD(originalRDD)
    mergeRDD = clickRDD.flatMap(
                lambda line: videoMapper(line)
            ).reduceByKey(add)
    videoStat = mergeRDD.collect()
    userChannelCount = {}
    for youtube_channel_id,count in videoStat:
        userChannelCount[youtube_channel_id] = count
    return userChannelCount

def update_channel_ratio(redisCli, userChannelCount):
    total = 0;
    channelDct = {}

    db = MySQLdb.connect("10.8.22.123", "banews_w", "MhxzKhl-Happy", "banews")
    timelimit = int(time.time()) - 30*24*3600
    sql = '''select distinct youtube_channel_id from `banews`.`tb_video` where is_valid=1 and status>=1 and update_time>%d''' % timelimit
    cursor = db.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    for channel_id, count in userChannelCount.items():
        total += count
    total += len(results) - len(userChannelCount)

    for result in results:
        channel_id = result[0]
        count = 1
        if channel_id in userChannelCount:
            count = userChannelCount[channel_id]
        channelDct[channel_id] = count*1.0/total
    update_log("/home/work/zyl/banews-alg/userTopicInterest/log/", "31999", channelDct)
    redisCli.hmset('ALG_YOUTUBE_CHANNEL_RATIO_KEY', channelDct)


def update_log(directory, channelId, userChannelCount):
    fileName = os.path.join(directory, '%s_%s_SUCCESS.dat' \
                            % (datetime.now().strftime('%Y-%m-%d_%H:%M'), channelId))
    fp = open(fileName, "w")
    for youtube_channel_id, count in userChannelCount.items():
        fp.write("%s\t%s\n"%(youtube_channel_id, str(count)))
    fp.close()

if __name__ == '__main__':
    now = datetime.now()
    end = now.date() + timedelta(days=1)
    start = now.date() - timedelta(days=7)
    getVideoChannelMap()
    fileLst = getFileList(start, end)
    userChannelCount = video_stat(fileLst, start, end)


    redisCli_online = Redis(host='10.8.15.189', port=6379)
    redisCli_sandbox = Redis(host='10.8.14.136', port=6379)
    update_channel_ratio(redisCli_sandbox, userChannelCount)
    update_channel_ratio(redisCli_online, userChannelCount)