# -*- coding:utf-8 -*-
import os
import json
import sys
import MySQLdb
import traceback
from pyspark import SparkContext
from datetime import date, datetime, timedelta
from operator import itemgetter, add
from numpy import mean, median
import logging

LOG_DIR = '/banews/useraction'

sc = SparkContext(appName='dailystat/lantian')
conn = MySQLdb.connect(host="10.8.22.123", user="banews_w", passwd="MhxzKhl-Happy", port=3306, db="banews-report")
conn.autocommit(True)

UV_EVENT_SET = set(["020103", "020104", "020301"])
PV_EVENT_SET = set(["020103", "020301"])
PUSH_EVENT_SET = set(["020106"])
INSTALL_EVENT_SET = set(["040101", "040102"])
FIRST_ONLINE_DAY = datetime(2016, 6, 2)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dailyStat")

class StatInfo(object):
    def __init__(self):
        self.datenum = 0
        self.utm_source = ""
        self.utm_medium = ""
        self.alluv = 0
        self.uv = 0
        self.install = 0
        self.mean_avg_read = 0.0
        self.median_avg_read = 0.0

def line_tran(line):
    try:
        info = json.loads(line)
        did = info["did"]
        return (did, info)
    except:
        return None

def getSpanRdd(start_date, end_date):
    fileLst = []
    cur_date = start_date
    while cur_date < end_date:
        curDir = os.path.join(LOG_DIR, cur_date.strftime('%Y/%m/%d'),
                'useraction.log-*')
        fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def revisit_trans(line):
    info = line[1]
    did = info['did']
    event_id = info['event-id']
    os = check_os_version(did)
    tm = datetime.fromtimestamp(int(info.get("time", 0))/1000)
    tm = datetime(tm.year, tm.month, tm.day)
    return did, (tm, (os, os))

def revistrate_tran(line):
    did = line[0]
    base_day = line[1][0][0]
    utm_source = line[1][0][1]
    cnt = line[1][1]
    if not cnt:
       cnt = 0
    cnt =  (1, cnt)
    return ((utm_source, base_day), cnt)

def uv_filter(line, start, end):
    if not line:
        return False
    info = line[1]
    tm = datetime.fromtimestamp(int(info.get("time", 0))/1000)
    did = info.get("did", None)
    if not did:
        return False
    if tm < start or tm >= end:
        return False
    event_id = info.get("event-id", None)
    if event_id in UV_EVENT_SET:
        return True
    return False

def action_filter(line, start, end):
    if not line:
        return False
    info = line[1]
    tm = datetime.fromtimestamp(int(info.get("time", 0))/1000)
    did = info.get("did", None)
    if not did:
        return False
    if tm < start or tm > end:
        return False
    event_id = info.get("event-id", None)
    if not event_id or event_id in PUSH_EVENT_SET:
        return False
    return True

def check_os_version(did):
    if "-" in did:
        return "ios"
    else:
        return "android"

def user_map(line):
    did = line[0]
    source = check_os_version(did)
    allinfo = line[1]
    ios_cnt = 0
    android_cnt = 0
    act_cnt = {"uv":0, "action_uv":0, "read":[0], "install":0}
    if allinfo:
        act_cnt["uv"] = 1
    newsset = set()
    for info in allinfo:
        event_id = info["event-id"]
        if event_id in UV_EVENT_SET:
            act_cnt["action_uv"] = 1
        if event_id in PV_EVENT_SET:
            news_id = info["news_id"]
            newsset.add(news_id)
            #act_cnt["read"][0] += 1
            act_cnt["read"][0] = len(newsset)
        if event_id in INSTALL_EVENT_SET:
            act_cnt["install"] = 1
    stat_info = [act_cnt.get("uv"), act_cnt.get("action_uv"), act_cnt.get("install"), act_cnt.get("read")]
    yield ((source, source), stat_info)
    yield (("all", "all"), stat_info)

def get_daily_stat(cur_day):
    start = cur_day
    end = cur_day + timedelta(days=1)
    fileLst = getSpanRdd(start, end)
    curFile = ",".join(fileLst)
    dataRDD = sc.textFile(curFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: action_filter(line, start, end)
            ).groupByKey(64).flatMap(
                lambda line: user_map(line)
            ).reduceByKey(
                lambda x,y: map(add, x, y)
            )
    allstat = dataRDD.collect()
    statinfo = {}
    for info in allstat:
        dailyInfo = StatInfo()
        key = info[0]
        stat = info[1]
        dailyInfo.datenum = cur_day.strftime("%Y%m%d")
        dailyInfo.utm_source = key[0]
        dailyInfo.utm_medium = key[1]
        dailyInfo.alluv = stat[0] 
        dailyInfo.uv = stat[1] 
        dailyInfo.install = stat[2] 
        dailyInfo.mean_avg_read = mean(stat[3])
        dailyInfo.median_avg_read = median(stat[3])
        statinfo[key] = dailyInfo
    return statinfo

def get_revisit_stat(cur_day, span=7):
    fileLst = getSpanRdd(cur_day, cur_day + timedelta(days=1))
    curFile = ",".join(fileLst)
    end_day = cur_day + timedelta(days=1)
    curRDD = sc.textFile(curFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: uv_filter(line, cur_day, end_day)
            ).map(
                lambda line: (line[1]['did'], 1)
            ).distinct(64)

    start_day = max(cur_day - timedelta(days=span), FIRST_ONLINE_DAY)
    end_day = cur_day


    fileLst = getSpanRdd(start_day, end_day)
    hisFile = ",".join(fileLst)
    hisRDD = sc.textFile(hisFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: uv_filter(line, start_day, end_day)
            ).map(
                lambda line: revisit_trans(line)
            ).distinct(40).flatMap(
                lambda line: (line, (line[0], (line[1][0], ("all", "all"))))
            )

    hisRDD = hisRDD.leftOuterJoin(curRDD).map(
                lambda line: revistrate_tran(line)
             ).reduceByKey(
                     lambda x,y: (x[0] + y[0], x[1] + y[1])
             )
    allstat = hisRDD.collect()
    time_stat = {}
    for source, stat in allstat:
        utm_source = source[0]
        base_day = source[1]
        rate = stat[1]/float(stat[0])
        tmp = time_stat.setdefault(base_day, {})#.append([utm_source, rate])
        tmp[utm_source] = rate
    sort_key = sorted(time_stat.keys())
    #for tm in sort_key:
    #    print tm
    #    stat = time_stat[tm]
    #    for info in stat:
    #        print info
    return time_stat

def dump_daily_mysql_info(stat_info):
    cursor = conn.cursor()
    sqlcmd = '''
    INSERT INTO tb_daily_report(
    datenum,
    utm_source,
    utm_medium,
    install,
    alluv,
    uv,
    mean_avg_read,
    median_avg_read
    )
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    for source, stat in stat_info.iteritems():
        cmd = sqlcmd
        sqldata = (stat.datenum, stat.utm_source, stat.utm_medium, stat.install, stat.alluv, stat.uv, stat.mean_avg_read,\
                stat.median_avg_read)
        #print sqlcmd % sqldata
        cursor.execute(cmd, sqldata)

def update_revisit_info(curday, time_stat):
    for tm, stat in time_stat.iteritems():
        delta_day = (curday.date() - tm.date()).days
        update_mysql_revisit_info(tm, delta_day, stat)



def update_mysql_revisit_info(day, num, stat_info):
    cursor = conn.cursor()
    update_column = "ratio%s"%num
    sqlcmd = "UPDATE tb_daily_report SET %s"%(update_column)
    sqlcmd += "=%s WHERE utm_source=%s and utm_medium=%s and datenum=%s"
    select_cmd = "SELECT id FROM tb_daily_report WHERE utm_source=%s and utm_medium=%s and datenum=%s"
    datenum = day.strftime("%Y%m%d")
    for source, rate in stat_info.iteritems():
        utm_source = source[0]
        utm_medium = source[1]
        selectdata = (utm_source, utm_medium, datenum)
        res = cursor.execute(select_cmd, selectdata)
        if res == 0:
            msg = "NO DATA in database for date %s utm_source %s utm_medium %s"%(datenum, utm_source, utm_medium)
            logger.warn(msg)
            continue
        sqldata = (rate, utm_source, utm_medium, datenum)
        #print sqlcmd % sqldata
        res = cursor.execute(sqlcmd, sqldata)

if __name__ == '__main__':
    now= datetime.now()
    end_day = datetime(now.year, now.month, now.day)
    start_day = end_day - timedelta(days=1)

    cur_day = start_day
    while cur_day < end_day:
        print cur_day
        daily_stat = get_daily_stat(cur_day)
        dump_daily_mysql_info(daily_stat)
        revist_stat = get_revisit_stat(cur_day, span=7)
        update_revisit_info(cur_day, revist_stat)
        cur_day += timedelta(days=1)

