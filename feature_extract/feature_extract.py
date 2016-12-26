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


LIST_EVENT_SET = set(["020104"])
DETAIL_EVENT_SET = set(["020103", "020301"])

BS_FEATURE_DISPLAY_TML = "BS_FEATURE_DISPLAY_%s"
BS_FEATURE_CLICK_TML = "BS_FEATURE_CLICK_%s"

SHOW_TYPE = 1
CLICK_TYPE = 0

FEATURE_MAP = {SHOW_TYPE:BS_FEATURE_DISPLAY_TML, CLICK_TYPE:BS_FEATURE_CLICK_TML}
REDIS_EXPIRE_TIME = 24 * 2 * 3600

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
    if event_id in LIST_EVENT_SET or event_id in DETAIL_EVENT_SET:
        return True
    return False

def getTransferTime(timestamp):
    if timestamp:
        tm = datetime.fromtimestamp(float(timestamp)/1000.)
        tm = datetime(tm.year, tm.month, tm.day)
        return tm
    else:
        return None

def getSpanFiles(start_date, end_date):
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

def line_mapper(line):
    return (
            line.get('event-id'),
            line.get('news_id'),
            line.get('news'),
            )

def feature_map(line):
    event_id, news_id, news= line
    #0 mean show, 1 mean click
    if event_id in LIST_EVENT_SET:
        if news:
            for nid in news:
                yield ((nid, SHOW_TYPE), 1)
        
    if event_id in DETAIL_EVENT_SET:
        if news_id:
            yield ((news_id, CLICK_TYPE), 1)


def feature_extract(fileLst, start, end):
    allfile = ','.join(fileLst)
    featureRDD = sc.textFile(allfile).map(
            lambda dctStr: line_tran(dctStr)
        ).filter(
            lambda line: action_filter(line, start, end)
        ).map(
            lambda line: line_mapper(line)
        ).flatMap(
            lambda line: feature_map(line)
        ).reduceByKey(lambda x,y: x+y)
        
    feature = featureRDD.collect()
    return feature

def update_redis(redisCli, features):
    for info, cnt in features:
        nid, _tp = info
        redis_perfix = FEATURE_MAP[_tp]
        key = redis_perfix % nid
        redisCli.set(key, cnt, ex=REDIS_EXPIRE_TIME)

    
if __name__ == '__main__':
    now = datetime.now()
    end = now.date() + timedelta(days=1)
    start = now.date() - timedelta(days=15) 
    fileLst = getSpanFiles(start, end)
    allfeatures = feature_extract(fileLst, start, end)
    redisCli_online = Redis(host='10.8.7.6', port=6379)
    redisCli_sandbox = Redis(host='10.8.14.136', port=6379)
    update_redis(redisCli_sandbox, allfeatures)

