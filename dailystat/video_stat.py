# -*- coding:utf-8 -*-
import os
import json
import traceback
import sys
import MySQLdb
from pyspark import SparkContext
from datetime import date, datetime, timedelta
from operator import itemgetter
from numpy import mean, median

LOG_DIR = '/banews/useraction'
LIST_REQUEST_CODE = '020104'
LIST_ARTICLE_REQUEST_CODE = '020103'
FIRST_ONLINE_DAY = datetime(2016, 6, 2)

sc = SparkContext(appName='channelStat/lantian')
conn = MySQLdb.connect(host="10.8.22.123", user="banews_w", passwd="MhxzKhl-Happy", port=3306, db="banews-report")
conn.autocommit(True)
SHOW_ACTION = '020104'
CLICK_ACTION  = '020103'
VIDEO_PLAY_ACTION = "020301"
PUSH_CLICK_ACTION = '020105'
CLICK_ACTION_SET = set([CLICK_ACTION, VIDEO_PLAY_ACTION])
ACTION_SET = set([SHOW_ACTION, CLICK_ACTION, VIDEO_PLAY_ACTION, PUSH_CLICK_ACTION])

class VideoStat(object):
    def __init__(self):
        self.url_sign = ""
        self.youtube_id = ""
        self.title = ""
        self.datenum = ""
        self.youtube_channel_id = ""
        self.youtube_channel_name = ""
        self.show_num = 0
        self.click_num = 0

def line_tran(line):
    try:
        return json.loads(line)
    except:
        return {}

def getSpanFileLst(start_date, end_date):
    fileLst = []
    cur_date = start_date
    while cur_date < end_date:
        curDir = os.path.join(LOG_DIR, cur_date.strftime('%Y/%m/%d'),
                'useraction.log-*')
        fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getTransferTime(timestamp):
    if timestamp:
        tm = datetime.fromtimestamp(float(timestamp)/1000.)
        tm = datetime(tm.year, tm.month, tm.day)
        return tm
    else:
        return None

def action_filter(line, start, end):
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000)
    did = line.get("did", None)
    if not did:
        return False
    if tm < start or tm >= end:
        return False
    event_id = line.get("event-id", None)
    if event_id in ACTION_SET:
        if event_id == SHOW_ACTION:
            channel_id = line.get("channel_id")
            if channel_id == "30001":
                return True
            else:
                return False
        else:
            return True
    return False

def line_mapper(line):
    return (
            line.get('event-id'), 
            line.get('did'),
            line.get('news_id'),
            {line.get('prefer'):1},
            line.get('channel_id'),
            getTransferTime(line.get('time')),
            line.get('news'),
            )


def getDisplayRDD(originalRDD):
    displayRDD = originalRDD.filter(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    eventId==SHOW_ACTION and prefer and newsLst
            ).map(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    ((time, did, channelId), set(newsLst))
            ).reduceByKey(
                lambda x,y: x | y
            ).map(
            lambda ((time, did, channelId), newsLst):
                ((time, did), (channelId, newsLst))
            )
    return displayRDD

def getClickRDD(originalRDD):
    clickRDD = originalRDD.filter(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    eventId in CLICK_ACTION_SET
        ).map(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    ((time, did), set([newsId, ]))
        ).reduceByKey(
            lambda x, y: x | y
        )
    return clickRDD

def articleMapper_1(line):
    tm, did = line[0]
    channelId, show = line[1][0]
    click = line[1][1]
    if not click:
        click = set()
    tmp = click & show
    if len(tmp) == 0:
        return (1,0)
    return (1,1)

def articleMapper(line):
    tm, did = line[0]
    channelId, show = line[1][0]
    click = line[1][1]
    if not click:
        click = set()
    tmp = click & show
    for nid in show:
        yield ((nid, tm), (1, 0))
    for nid in tmp:
        yield ((nid, tm), (0, 1))

def batch_get_news(cursor):
    news_source = {}
    cursor.execute("select min(id), max(id) from tb_news")
    return news_source


def get_news_source(allid):
    conn1 = MySQLdb.connect(host="10.8.22.123", user="banews_w", passwd="MhxzKhl-Happy", port=3306, db="banews")
    cursor = conn1.cursor()
    news_source = {}
    if len(allid)==0:
        news_source = batch_get_news(cursor)
    else:
        for nid in allid:
            try:
                cursor.execute("select youtube_video_id, youtube_channel_id, youtube_channel_name, title from tb_video where news_url_sign=%s", (nid,))
                youtube_id, youtube_channel_id, youtube_channel_name, title = cursor.fetchone()
                news_source[nid] = (youtube_channel_id, youtube_channel_name, youtube_id, title)
            except:
                traceback.print_exc()
                continue
    return news_source

def dump_article_stat_database(tmnum, info, news_source, tp, topk):
    cursor = conn.cursor() 
    res = info[0:topk]

    sqlcmd = """
    INSERT INTO tb_channel_mostread_stat_report(`datenum`, `url_sign`, `title`, `channel_id`, `source`, `show_num`, `click_num`, `click_rate`, `type`, `is_push`)\
           VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for nid, channelId, show, click, rate, is_push in res: 
        source = news_source.get(nid)
        if not source:
            continue
        src, title = source
        sqldata = (tmnum, nid, title, channelId, src, show, click, rate, tp, is_push) 
        try:
            cursor.execute(sqlcmd, sqldata)
        except:
            print sqlcmd % sqldata    

def calVideoStat(start, end):
    fileLst = getSpanFileLst(start, end)
    originalRDD = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: line_tran(dctStr)
        ).filter(
            lambda line: action_filter(line, start, end)
        ).map(
            lambda line: line_mapper(line)
        ).cache()
    displayRDD = getDisplayRDD(originalRDD)
    clickRDD = getClickRDD(originalRDD)

    mergeRDD = displayRDD.leftOuterJoin(clickRDD, 64).flatMap(
                lambda line: articleMapper(line)
            ).reduceByKey(
                lambda x, y: (x[0] + y[0], x[1] + y[1])
            )
    articleStat = mergeRDD.collect() 
    allid = [info[0][0] for info in articleStat]
    #for info in articleStat:
    #    nid, tm = info[0][0], info[0][1] 
    #    show, click = info[1][0], info[1][1]
    #    print nid, tm, show, click, float(click)/show
    news_source = get_news_source(allid)
    print len(news_source)
    source_stat = {}
    article_data = [] 
    for info in articleStat:
        stat = VideoStat()
        nid, tm = info[0][0], info[0][1] 
        if tm.strftime('%s') != start.strftime('%s'):
            continue
        show, click = info[1][0], info[1][1]
        try:
            youtube_channel_id, youtube_channel_name, youtube_id, title = news_source[nid]
        except:
            continue
        stat.url_sign = nid
        stat.youtube_id = youtube_id
        stat.youtube_channel_id = youtube_channel_id
        stat.youtube_channel_name = youtube_channel_name
        stat.title = title
        stat.datenum = int(tm.strftime('%Y%m%d'))
        stat.show_num = show
        stat.click_num = click
        article_data.append(stat)

    dump_video_stat(article_data)

def dump_video_stat(video_stat):
    cursor = conn.cursor()
    dailysqltmp = "INSERT INTO tb_video_daily_stat(url_sign, youtube_id, title, datenum, youtube_channel_id, youtube_channel_name,\
    show_num, click_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"

    sqltmp = "INSERT INTO tb_video_stat(url_sign, youtube_id, title, youtube_channel_id, youtube_channel_name,\
    show_num, click_num) VALUES(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE show_num=show_num+%s, click_num=click_num+%s"
    for stat in video_stat:
        dailysqldata = (stat.url_sign, stat.youtube_id, stat.title, stat.datenum, stat.youtube_channel_id, stat.youtube_channel_name,\
                stat.show_num, stat.click_num)

        sqldata = (stat.url_sign, stat.youtube_id, stat.title,  stat.youtube_channel_id, stat.youtube_channel_name,\
                stat.show_num, stat.click_num, stat.show_num, stat.click_num)

        try:
            cursor.execute(dailysqltmp, dailysqldata)
            cursor.execute(sqltmp, sqldata)
        except MySQLdb.IntegrityError, e:
            print "%s already exist in database"%(stat.url_sign)
            continue

 
if __name__ == '__main__':
    now = datetime.now()  
    end = datetime(now.year, now.month, now.day)
    start = end - timedelta(days=1)
    curday = start
    while curday < end:
        calVideoStat(curday, curday+timedelta(days=1))
        curday += timedelta(days=1)

