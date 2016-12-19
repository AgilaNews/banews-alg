import os
from redis import Redis
import json
from datetime import date, datetime, timedelta
from pyspark import SparkContext
from numpy import mean, median
from urllib import quote, unquote
from operator import itemgetter

TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
LOG_DIR = '/banews/useraction'
URL_TMP = "http://share.agilanews.today/news?id=%s"
sc = SparkContext(appName='hotvideo/lantian')

VIDEO_DISPATCH_ACTION = set(['020104'])
VIDEO_PLAY_ACTION = set(['020301','020102'])

REDIS_POPULAR_NEWS_PREFIX = 'BA_POPULAR_NEWS_%s'

def line_tran(line):
    try:
        return json.loads(line)
    except:
        return {}

def action_filter(line, start, end):
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000).date()
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
        if channel_id == "30001":
            return True
    if event_id in VIDEO_PLAY_ACTION:
        channel_id = line.get("refer")
        if channel_id == "30001":
            return True
        return True
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
            getTransferTime(line.get('time')),
            line.get('news'),
            )

def getDisplayRDD(originalRDD):
    displayRDD = originalRDD.filter(
            lambda (eventId, did, newsId, session, time, newsLst): \
                    eventId in VIDEO_DISPATCH_ACTION and newsLst
            ).map(
            lambda (eventId, did, newsId, session, time, newsLst): \
                    (did, set([str(n) for n in newsLst]))
            ).reduceByKey(
                lambda x,y: x | y
            )    
    return displayRDD

def getClickRDD(originalRDD):
    clickRDD = originalRDD.filter(
            lambda (eventId, did, newsId, session, time, newsLst): \
                    eventId in VIDEO_PLAY_ACTION 
        ).map(
            lambda (eventId, did, newsId, session, time, newsLst): \
                    (did, set([str(newsId), ]))
        ).reduceByKey(
            lambda x, y: x | y
        )
    return clickRDD

def getSpanRdd(start_date, end_date):
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

def articleMapper(line):
    did = line[0]
    show = line[1][0]
    click = line[1][1]
    if not click:
        click = set()
    tmp = click & show
    for nid in show:
        yield (nid, (1, 0))
    for nid in tmp:
        yield (nid, (0, 1))

def video_stat(fileLst, start, end):
    allfile = ','.join(fileLst)
    originalRDD = sc.textFile(allfile).map(
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
    hotvideo = []
    for nid, res in articleStat:
        show, click = res
        if show <= 100:
            continue
        rate = float(click)/show
        urlsign = quote(nid)
        url = URL_TMP % urlsign
        hotvideo.append([nid, url, rate, show, click])
    hotvideo = sorted(hotvideo, key=itemgetter(2), reverse=True)
    return hotvideo
    #cnt = 1
    #for nid, url, rate, show,click in hotvideo:
    #    print cnt, nid, url, rate, show, click
    #    cnt += 1

def update_log(directory, channelId, hotvideo):
    fileName = os.path.join(directory, '%s_%s_SUCCESS.dat' \
                            % (datetime.now().strftime('%Y-%m-%d_%H:%M'), channelId))
    fp = open(fileName, "w")
    for nid, url, rate, show,click in hotvideo:
        fp.write("%s\t%s\t%s\t%s\t%s\n"%(nid, url, rate, show, click))
    fp.close()


def update_video_hot_queue(hotvideo, channelId, redisCli):
    hotkey = REDIS_POPULAR_NEWS_PREFIX % channelId
    if redisCli.exists(hotkey):
        redisCli.delete(hotkey)
    for nid, url, rate, show, click in hotvideo[0:100]:
        redisCli.rpush(hotkey, nid)


if __name__ == '__main__':
    now = datetime.now()
    end = now.date() + timedelta(days=1)
    start = now.date() - timedelta(days=3) 
    fileLst = getSpanRdd(start, end)
    hotvideo = video_stat(fileLst, start, end)
    redisCli_online = Redis(host='10.8.7.6', port=6379)
    redisCli_sandbox = Redis(host='10.8.16.33', port=6379)
    update_log("/home/work/banews-alg/video/log/", "30001", hotvideo)
    update_video_hot_queue(hotvideo, "30001", redisCli_sandbox)
    update_video_hot_queue(hotvideo, "30001", redisCli_online)

