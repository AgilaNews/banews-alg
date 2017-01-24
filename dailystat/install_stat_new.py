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

sc = SparkContext(appName='newUserInstallnew/lantian')
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

            #trick for wrong package name for oppo_offline_cedu (the right pacakage should be oppo_offline_cebu)
            if utm_campaign == "cedu":
                utm_campaign = "cebu"

            utm_source = (utm_source, utm_medium, utm_campaign)
        except:
            traceback.print_exc()
            utm_source = ("unknown", "", "")

    did = line.get("did")
    device = "android"
    if "-" in did:
        device = "iOS"
    utm_source = utm_source + (device, )
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

def get_user_from_mysql(cur_day, span):
    end_day = cur_day 
    start_day = max(cur_day - timedelta(days=span), FIRST_ONLINE_DAY)

    end_day = int(end_day.strftime("%Y%m%d"))
    start_day = int(start_day.strftime("%Y%m%d"))
    cursor = conn.cursor()
    sqlcmd = "select device_id, utm_source, utm_medium, utm_campaign, device_type, datenum from tb_all_install_user where datenum>=%s and datenum <%s"
    sqldata = (start_day, end_day)
    cursor.execute(sqlcmd, sqldata)

    allres = []
    for device_id, utm_source, utm_medium, utm_campaign, device_type, datenum in cursor.fetchall():
        tm = datetime.strptime(str(datenum),"%Y%m%d")
        key = (tm, (utm_source, utm_medium, utm_campaign, device_type)) 
        allkey = (tm, ("all", "all", "all", "all"))
        allres.append([device_id, key])
        allres.append([device_id, allkey])
    return allres


def calRevistRate(cur_day, span=90):
    #get history RDD from database
    allres = get_user_from_mysql(cur_day, span) 
    hisRDD = sc.parallelize(allres, 64)
    #get today's revisit RDD
    fileLst = getSpanRdd(cur_day, cur_day + timedelta(days=1))
    curFile = ",".join(fileLst)
    curRDD = sc.textFile(curFile).map(
                lambda line: line_tran(line)
            ).filter(
                lambda line: uv_filter(line, cur_day)     
            ).map(
                lambda line: (line['did'], 1)
            ).distinct(64)
    #cal revist
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


def update_daily_install_user(cur_day, newuser):
    cursor = conn.cursor()
    sqlcmd = """ 
    INSERT INTO tb_all_install_user(`device_id`, `utm_source`, `utm_medium`, `utm_campaign`, `device_type`, `datenum`, `install_time`) VALUES(%s, %s, %s, %s, %s, %s, %s)
    """
    source_user = {}
    for info in newuser:
        did = info[0]
        tm = info[1][0]
        (utm_source, utm_medium, utm_campaign, device) = info[1][1]
        sqldata =(did, utm_source, utm_medium, utm_campaign, device, tm.date().strftime('%Y%m%d'), tm.strftime('%s'))
        try:
            cursor.execute(sqlcmd, sqldata)
            source_user.setdefault((utm_source, utm_medium, utm_campaign, device), set()).add(did)
            source_user.setdefault(("all", "all", "all", "all"), set()).add(did)
        except MySQLdb.IntegrityError, e:
            continue
    return source_user

def update_daily_info(cur_day, newuser):
    cursor = conn.cursor() 
    source_user = update_daily_install_user(cur_day, newuser) 
    
    monthly_sqlcmd = "select utm_source, utm_medium, utm_campaign, device_type, count(distinct(device_id)) from tb_all_install_user where datenum<=%s and datenum>=%s group by CONCAT(utm_source, utm_medium, utm_campaign, device_type)"
    total_sqlcmd = "select utm_source, utm_medium, utm_campaign, device_type, count(distinct(device_id)) from tb_all_install_user group by CONCAT(utm_source, utm_medium, utm_campaign, device_type)"

    all_monthly_sqlcmd = "select count(distinct(device_id)) from tb_all_install_user where datenum<=%s and datenum>=%s"
    all_total_sqlcmd = "select count(distinct(device_id)) from tb_all_install_user"

    source_monthly = {}
    end = int(cur_day.strftime('%Y%m%d'))
    start  = int(datetime(cur_day.year, cur_day.month, 1).strftime('%Y%m%d'))
    monthly_data = (end, start)
    cursor.execute(monthly_sqlcmd, monthly_data)
    for utm_source, utm_medium, utm_campaign, device_tp, cnt in cursor.fetchall():
        source = (utm_source, utm_medium, utm_campaign, device_tp)
        source_monthly[source] = cnt
    cursor.execute(all_monthly_sqlcmd, monthly_data) 
    source_monthly[('all', "all", "all", "all")] = cursor.fetchone()[0]

    source_total = {}
    cursor.execute(total_sqlcmd)
    for utm_source, utm_medium, utm_campaign, device_tp, cnt in cursor.fetchall():
        source = (utm_source, utm_medium, utm_campaign, device_tp)
        source_total[source] = cnt 

    cursor.execute(all_total_sqlcmd)
    source_total[('all', "all", "all", "all")] = cursor.fetchone()[0]

    sqlcmd = '''
    INSERT INTO tb_install_user_daily_report(
    datenum,
    utm_source,
    utm_medium,
    utm_campaign,
    device_type,
    install,
    monthly_install,  
    total_install
    )
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    for source, user in source_user.iteritems():
        sqldata = (cur_day.strftime('%Y%m%d'), source[0], source[1], source[2], source[3], len(user), \
                source_monthly.get(source, 0), source_total.get(source, 0))
        try:
            #print sqlcmd % sqldata
            cursor.execute(sqlcmd, sqldata)
        except MySQLdb.IntegrityError, e:
            msg = "day %s stat info is already in database"%(cur_day)
            print msg

def update_product_stat(cur_day):
    cursor = conn.cursor()
    datenum = cur_day.strftime('%Y%m%d')
    sqltmp = "select count(*) from tb_all_install_user where datenum=%s and device_type=%s";
    androiddata = (datenum, "android")
    cursor.execute(sqltmp, androiddata)
    res = cursor.fetchone()
    andorid_count = 0
    if res:
        andorid_count = res[0]

    iosdata = (datenum, "ios")
    cursor.execute(sqltmp, iosdata)
    res = cursor.fetchone()
    ios_count = 0
    if res:
        ios_count = res[0]
    all_count = ios_count + andorid_count
    print ios_count, andorid_count, all_count
    updatesql = "update tb_daily_report set install=%s where utm_source=%s and datenum=%s"
    cursor.execute(updatesql, (andorid_count, "android", datenum))
    cursor.execute(updatesql, (ios_count, "ios", datenum))
    cursor.execute(updatesql, (all_count, "all", datenum))


def update_revisit_info(cur_day, revisit_stat):
    cursor = conn.cursor()
    selectcmd = '''select id, history_ratio from tb_install_user_daily_report WHERE utm_source=%s and utm_medium=%s 
                    and utm_campaign=%s and device_type=%s and datenum=%s'''
    for tm, stat in revisit_stat.iteritems():
        #we put 1-7 day revisit ratio in db column, and all revisti info in json
        span = (cur_day - tm).days
        if span > 7:
            updatecmd = '''UPDATE tb_install_user_daily_report SET history_ratio=%s WHERE id=%s'''
        else:
            update_column = "ratio%s"%(cur_day - tm).days
            updatecmd = "UPDATE tb_install_user_daily_report SET %s"%(update_column)
            updatecmd += "=%s,history_ratio=%s WHERE id=%s"
        datenum = tm.strftime('%Y%m%d')
        for source, rate in stat:
            selectdata = (source[0], source[1], source[2], source[3], datenum)   
            try:
                res = cursor.execute(selectcmd,  selectdata)
            except:
                continue
            _id, history_ratio = cursor.fetchone()
            if not history_ratio: #first time update
                history_ratio = '{}'
            json_res = json.loads(history_ratio)
            json_res[str(span)] = "%.3f"%rate
            json_res = json.dumps(json_res)
            if span > 7:
                updatedata = (json_res, _id)
            else:
                updatedata = (rate, json_res, _id)
            #print updatecmd % updatedata
            cursor.execute(updatecmd, updatedata)
            
if __name__ == '__main__':
    now= datetime.now()  
    end_day = datetime(now.year, now.month, now.day)
    start_day = end_day - timedelta(days=1)
    #start_day = FIRST_ONLINE_DAY
    #end_day = datetime(now.year, now.month, now.day)
    cur_day = start_day
    while cur_day < end_day:
        print cur_day
        newuser = getDailyNewUser(cur_day) 
        update_daily_info(cur_day, newuser)
        update_product_stat(cur_day)
        stat = calRevistRate(cur_day, span=90)
        update_revisit_info(cur_day, stat)
        cur_day += timedelta(days=1)
    
