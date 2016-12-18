# -*- coding:utf-8 -*-
import os
import json
import sys
import MySQLdb
import traceback
from pyspark import SparkContext
from datetime import date, datetime, timedelta
from operator import itemgetter

LOG_DIR = '/banews/useraction'
LIST_REQUEST_SERVER_CODE = '020104'
FRESH_CLIENT_CODE = '040101'
FRESH_SERVER_CODE = '040102'
INSTALL_EVENT_SET = set([FRESH_SERVER_CODE, FRESH_CLIENT_CODE])
UV_EVENT_SET = set(['020104', '020103','020301'])
FIRST_ONLINE_DAY = datetime(2016, 6, 21)

sc = SparkContext(appName='newUserInstall/lantian')
conn = MySQLdb.connect(host="10.8.22.123", user="banews_w", passwd="MhxzKhl-Happy", port=3306, db="banews-report")
conn.autocommit(True)

def line_tran(line):
    try:
        return json.loads(line)
    except:
        return {}

def getSpanRdd(start_date, end_date):
    fileLst = []
    cur_date = start_date
    while cur_date < end_date:
        curDir = os.path.join(LOG_DIR, cur_date.strftime('%Y/%m/%d'),
                'useraction.log-*')
        fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def newuser_filter(line, start, end):
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000)
    if tm < start or tm >= end :
        return False
    did = line.get("did", None)
    if not did:
        return False
    event_id = line.get("event-id", None)
    if event_id in INSTALL_EVENT_SET: 
        return True
    return False

def uv_filter(line, day):
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000)
    did = line.get("did", None)
    if not did:
        return False
    if tm < day:
        return False
    event_id = line.get("event-id", None)
    if event_id in UV_EVENT_SET:
        return True
    return False

def source_extract(line):
    ref = line.get("referrer", None)
    utm_source = ""
    if not ref:
        utm_source = ("unknown", "", "")
    else:
        try:
            refer_info = dict([a.split('=') for a in ref.split('&')])
            utm_source = refer_info.get("utm_source", "unknown")
            utm_source = utm_source.lower()
            if utm_source == "(not%20set)":
                utm_source = "facebook"
            #trick for offline package bug, all cherrymobile utm_source should be cosmic
            if utm_source == "cherrymobile":
                utm_source = "cosmic"

            utm_medium = refer_info.get("utm_medium", "")
            utm_medium = utm_medium.lower()
            if utm_medium == "(not%20set)":
                utm_medium = "facebook"

            utm_campaign = refer_info.get("utm_campaign", "")
            utm_campaign = utm_campaign.lower()

            utm_source = (utm_source, utm_medium, utm_campaign)
        except:
            traceback.print_exc()
            utm_source = ("unknown", "", "")
    return utm_source


def newuser_trans(line):
    utm_source = source_extract(line)

    did = line['did']
    event_id = line['event-id']
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000)
    #utm_source = utm_source.lower()
    return (did, (tm, utm_source))

def revisit_trans(line):
    did = line['did']
    event_id = line['event-id']
    tm = datetime.fromtimestamp(int(line.get("time", 0))/1000)
    tm = datetime(tm.year, tm.month, tm.day)
    utm_source = source_extract(line)
    return did, (tm, utm_source)

def revistrate_tran(line):
    did = line[0]
    base_day = line[1][0][0]
    utm_source = line[1][0][1]
    cnt = line[1][1]
    if not cnt:
       cnt = 0 
    cnt =  (1, cnt)
    return ((utm_source, base_day), cnt)
    

def getDailyNewUser(cur_day):
    start = cur_day
    end = cur_day + timedelta(days=1)
    fileLst = getSpanRdd(cur_day, end)
    fileLst = ",".join(fileLst)
    rdd = sc.textFile(fileLst).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: newuser_filter(line, start, end)     
            ).map(
                lambda line: newuser_trans(line) 
            ).groupByKey(40).mapValues(
                lambda line: sorted(line, key=itemgetter(1))[0]
            )
    newuser = rdd.collect()
    return newuser

def calRevistRate(cur_day, span=7):
    fileLst = getSpanRdd(cur_day, cur_day + timedelta(days=1))
    curFile = ",".join(fileLst)
    curRDD = sc.textFile(curFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: uv_filter(line, cur_day)     
            ).map(
                lambda line: (line['did'], 1)
            ).distinct(64)

    start_day = max(cur_day - timedelta(days=span), FIRST_ONLINE_DAY)
    end_day = cur_day


    fileLst = getSpanRdd(start_day, cur_day)
    if len(fileLst) ==0:
        return {}
    hisFile = ",".join(fileLst)
    hisRDD = sc.textFile(hisFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: newuser_filter(line, start_day, end_day)
            ).map(
                lambda line: revisit_trans(line)
            ).groupByKey(40).mapValues(
                lambda line: sorted(line, key=itemgetter(1))[0]
            ).flatMap(
                lambda line: (line, (line[0], (line[1][0], ("all", "all", "all"))))
            )

    #hisRDD = hisRDD.leftOuterJoin(curRDD)
    hisRDD = hisRDD.leftOuterJoin(curRDD).map(
                lambda line: revistrate_tran(line)
             ).reduceByKey(
                     lambda x,y: (x[0] + y[0], x[1] + y[1])
             )
    stat = hisRDD.collect()
    time_stat = {}
    for source, stat in stat:
        utm_source = source[0]
        base_day = source[1]
        rate = stat[1]/float(stat[0])
        time_stat.setdefault(base_day, []).append([utm_source, rate])
    return time_stat


def update_daily_info(cur_day, newuser):
    sqlcmd = """ 
    INSERT INTO tb_install_user(`device_id`, `utm_source`, `utm_medium`, `utm_campaign`, `datenum`, `install_time`) VALUES(%s, %s, %s, %s, %s, %s)
    """

    #allusercmd = """select device_id from tb_install_user"""
    cursor = conn.cursor()
    #cursor.execute(allusercmd)
    #allinstall = set()
    #for did in cursor.fetchall():
    #    allinstall.add(did[0])

    source_user = {}
    for info in newuser:
        did = info[0]
        #if did in allinstall:
        #    continue
        tm = info[1][0]
        utm_source = info[1][1][0]
        utm_medium = info[1][1][1]
        utm_campaign = info[1][1][2]
        sqldata =(did, utm_source, utm_medium, utm_campaign, tm.date().strftime('%Y%m%d'), tm.strftime('%s'))
        source_user.setdefault((utm_source, utm_medium, utm_campaign), set()).add(did)
        source_user.setdefault(("all", "all", "all"), set()).add(did)
        cursor.execute(sqlcmd, sqldata)

    monthly_sqlcmd = "select utm_source, utm_medium, utm_campaign, count(distinct(device_id)) from tb_install_user where datenum<=%s and datenum>=%s group by CONCAT(utm_source, utm_medium, utm_campaign)"
    total_sqlcmd = "select utm_source, utm_medium, utm_campaign, count(distinct(device_id)) from tb_install_user group by CONCAT(utm_source, utm_medium, utm_campaign)"

    all_monthly_sqlcmd = "select count(distinct(device_id)) from tb_install_user where datenum<=%s and datenum>=%s"
    all_total_sqlcmd = "select count(distinct(device_id)) from tb_install_user"

    source_monthly = {}
    end = int(cur_day.strftime('%Y%m%d'))
    start  = int(datetime(cur_day.year, cur_day.month, 1).strftime('%Y%m%d'))
    monthly_data = (end, start)
    cursor.execute(monthly_sqlcmd, monthly_data)
    for utm_source, utm_medium, utm_campaign, cnt in cursor.fetchall():
        source = (utm_source, utm_medium, utm_campaign)
        source_monthly[source] = cnt
    cursor.execute(all_monthly_sqlcmd, monthly_data) 
    source_monthly[('all', "all", "all")] = cursor.fetchone()[0]

    source_total = {}
    cursor.execute(total_sqlcmd)
    for utm_source, utm_medium, utm_campaign, cnt in cursor.fetchall():
        source = (utm_source, utm_medium, utm_campaign)
        source_total[source] = cnt 

    cursor.execute(all_total_sqlcmd)
    source_total[('all', "all", "all")] = cursor.fetchone()[0]

    sqlcmd = '''
    INSERT INTO tb_newuser_daily_report(
    datenum,
    utm_source,
    utm_medium,
    utm_campaign,
    install,
    monthly_install,  
    total_install
    )
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    '''
    for source, user in source_user.iteritems():
        sqldata = (cur_day.strftime('%Y%m%d'), source[0], source[1], source[2], len(user), \
                source_monthly.get(source, 0), source_total.get(source, 0))
        try:
            #print sqlcmd % sqldata
            cursor.execute(sqlcmd, sqldata)
        except MySQLdb.IntegrityError, e:
            msg = "day %s stat info is already in database"%(cur_day)
            print msg

def update_revisit_info(cur_day, revisit_stat):
    cursor = conn.cursor()
    selectcmd = "SELECT id FROM tb_newuser_daily_report WHERE utm_source=%s and utm_medium=%s and utm_campaign=%s and datenum=%s"
    for tm, stat in revisit_stat.iteritems():
        update_column = "ratio%s"%(cur_day - tm).days
        updatecmd = "UPDATE tb_newuser_daily_report SET %s"%(update_column)
        updatecmd += "=%s WHERE utm_source=%s and utm_medium=%s and utm_campaign=%s and datenum=%s"
        datenum = tm.strftime('%Y%m%d')
        for source, rate in stat:
            selectdata = (source[0], source[1], source[2], datenum)   
            res = cursor.execute(selectcmd,  selectdata)
            if res == 0:
                msg = "NO DATA in database for date %s utm_source %s"%(datenum, source)
                print msg
                continue
            updatedata = (rate, source[0], source[1], source[2], datenum)
            #print updatecmd % updatedata
            cursor.execute(updatecmd, updatedata)
            
if __name__ == '__main__':
    now= datetime.now()  
    end_day = datetime(now.year, now.month, now.day)
    start_day = end_day - timedelta(days=1)
    cur_day = start_day
    while cur_day < end_day:
        print cur_day
        newuser = getDailyNewUser(cur_day) 
        update_daily_info(cur_day, newuser)
        stat = calRevistRate(cur_day, span=7)
        update_revisit_info(cur_day, stat)
        cur_day += timedelta(days=1)
    
