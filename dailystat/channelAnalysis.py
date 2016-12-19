# -*- coding:utf-8 -*-
import os
import json
import sys
import MySQLdb
from pyspark import SparkContext
from datetime import date, datetime, timedelta
from operator import itemgetter

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

def displayReducer(x, y):
    dirx = x[0]
    diry = y[0]
    res = {}
    for direct, cnt in dirx.iteritems():
        res.setdefault(direct, 0) 
        res[direct] += cnt

    for direct, cnt in diry.iteritems():
        res.setdefault(direct, 0) 
        res[direct] += cnt
    return (res, x[1] | y[1])

def getDisplayRDD(originalRDD):
    displayRDD = originalRDD.filter(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    eventId==SHOW_ACTION and prefer and newsLst
            ).map(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    ((time, did, channelId), (prefer, set(newsLst)))
            ).reduceByKey(
                lambda x,y: displayReducer(x, y)
            ).map(
            lambda ((time, did, channelId), (prefer, newsLst)):
                ((time, did), (channelId, prefer, newsLst))
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

def rateMapper(line):
    tm, did = line[0]
    channelId, prefer, show = line[1][0]
    click = line[1][1]
    if not click:
        click = set()
    tmp = click & show
    pv = len(tmp)
    return ((tm, channelId), (1, pv, prefer, len(show), len(tmp), len(tmp)/float(len(show))))

def merge_dict(x, y):
    res = {}
    for k, v  in x.iteritems():
        res.setdefault(k, 0) 
        res[k] += v 

    for k, v in y.iteritems():
        res.setdefault(k, 0) 
        res[k] += v 
    return res

def statReducer(x, y):
    uv = x[0] + y[0]
    pv = x[1] + y[1] 
    req = merge_dict(x[2], y[2])
    show = x[3] + y[3]
    click = x[4] + y[4]
    rate = x[5] + y[5]
    return (uv, pv, req, show, click, rate)

def sourceMapper(line):
    tm, did = line[0]
    channelId, prefer, show = line[1][0]
    click = line[1][1]
    if not click:
        click = set()
    tmp = click & show
    pv = len(tmp)
    for nid in show:
        yield ((nid, tm, channelId), (1, 0))

    for nid in tmp:
        yield ((nid, tm, channelId), (0, 1))

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
                cursor.execute("select source_name, title from tb_news where url_sign=%s", (nid,))
                source, title = cursor.fetchone()
                news_source[nid] = (source.lower(), title)
            except:
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

def update_daily_channel_article_stat(tmnum, info, news_source, topk):
    channel_stat = {}
    for nid, channel_id, show, click, rate, is_push in info:
        if channel_id in ("10011", "10012"):
            continue
        channel_stat.setdefault(channel_id, []).append([nid, channel_id, show, click, rate, is_push])

    for channel_id, stat in channel_stat.iteritems():
        mostread = sorted(stat, key=itemgetter(3), reverse=True) 
        mostrate = sorted(stat, key=itemgetter(4), reverse=True) 
        dump_article_stat_database(tmnum, mostread, news_source, 0, topk)
        dump_article_stat_database(tmnum, mostrate, news_source, 1, topk)


def calChannelSourceStat(start_date, end_date):
    fileLst = getSpanFileLst(start_date, end_date)
    originalRDD = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: line_tran(dctStr)
        ).filter(
            lambda line: action_filter(line, start_date, end_date)
        ).map(
            lambda line: line_mapper(line)
        ).cache()
    displayRDD = getDisplayRDD(originalRDD)
    clickRDD = getClickRDD(originalRDD)
    mergeRDD = displayRDD.leftOuterJoin(clickRDD, 64).flatMap(
                lambda line: sourceMapper(line)
            ).reduceByKey(
                lambda x,y: (x[0] + y[0], x[1] + y[1])
            )    
    res = mergeRDD.collect()
    push_article = originalRDD.filter(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    eventId==PUSH_CLICK_ACTION
        ).map(
            lambda (eventId, did, newsId, prefer, channelId, time, newsLst): \
                    (newsId)
        ).distinct()

    push_article = set(push_article.collect())
    tm_res = {}
    for k, v in res:
        nid, tm, channelId = k
        show, click = v
        if show < 20:
            rate = 0.0
        else:
            rate = click/float(show)
        is_push = 0
        if nid in push_article:
            is_push = 1
        tm_res.setdefault(tm, []).append([nid, channelId, show, click, rate, is_push])

    sqlcmd = """
    INSERT INTO tb_channel_source_stat_report(`datenum`, `channel_id`, `source`, `article_num`, `click_article_num`, `show_num`, `click_num`)\
           VALUES(%s, %s, %s, %s, %s, %s, %s)
    """

    cursor = conn.cursor()
    kk = sorted(tm_res.keys()) 

    for tm in kk:
        res = tm_res[tm]
        nidkey = set([r[0] for r in res])
        news_source = get_news_source(nidkey)
        source_stat = {}
        tmnum = tm.strftime('%Y%m%d')
        update_daily_channel_article_stat(tmnum, res, news_source, 20)
        for nid, cid, show, click, r, is_push in res:
            source = news_source.get(nid)
            if not source:
                continue
            source = source[0]
            source_key = (cid, source)
            stat= source_stat.setdefault(source_key, [0, 0, 0, 0])
            stat[0] += 1
            if click > 0:
                stat[1] += 1
            stat[2] += show
            stat[3] += click
        for k, stat in source_stat.iteritems():
            try:
                sqldata = (tmnum, k[0], k[1], stat[0], stat[1], stat[2], stat[3]) 
                cursor.execute(sqlcmd, sqldata)
            except:
                print sqlcmd % sqldata

def calChannelStat(start_date, end_date):
    fileLst = getSpanFileLst(start_date, end_date)
    originalRDD = sc.textFile(','.join(fileLst)).map(
            lambda dctStr: line_tran(dctStr)
        ).filter(
            lambda line: action_filter(line, start_date, end_date)
        ).map(
            lambda line: line_mapper(line)
        ).cache()
    displayRDD = getDisplayRDD(originalRDD)
    clickRDD = getClickRDD(originalRDD)
    mergeRDD = displayRDD.leftOuterJoin(clickRDD, 64).map(
                lambda line: rateMapper(line)
            ).reduceByKey(
                lambda x, y : statReducer(x, y)
            )
    res = mergeRDD.collect()
    res = sorted(res)
    res = [(tt, (r[0], r[1], r[2], r[3], r[4], r[5]/r[0])) for tt, r in res]
    update_database(res)

def update_database(result):
    sqlcmd = """
    INSERT INTO tb_channel_stat_report(`datenum`, `channel_id`, `uv`, `pv`, `up_request`, `down_request`, `total_show`,\
            `total_click`, `user_avg_rate`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    for key, res in result:
        tm, channel = key;
        tmnum = tm.strftime('%Y%m%d')
        uv, pv, req, show, click, rate = res
        up = req.get('later')
        down = req.get('older')
        sqldata = (tmnum, channel, uv, pv, up, down, show, click, rate)
        try:
            cursor.execute(sqlcmd, sqldata)
        except:
            print sqlcmd % sqldata
            continue
    #for r in res:
    #    print r
 
if __name__ == '__main__':
    now = datetime.now()  
    end = datetime(now.year, now.month, now.day)
    start = end - timedelta(days=1)
    calChannelStat(start, end)
    calChannelSourceStat(start, end)

