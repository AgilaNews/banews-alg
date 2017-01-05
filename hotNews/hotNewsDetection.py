# -*- coding:utf-8 -*-
import os
import re
import sys
import json
import numpy
from datetime import date, datetime, timedelta
import MySQLdb
import urllib
from redis import Redis

import settings

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
ATTRIBUTE_DIM = 9
(NEWS_ID, SRC_URL, CHANNEL_ID, TITLE, SRC_NAME, \
        PUBLISH_TIME, FETCH_TIME, CONTENT, TYPE) = \
        range(ATTRIBUTE_DIM)

MINTHRE_HAMMING_DISTANCE = 4

log_file = open('hotnews.log','w')

def getSpanNews(start_date=None, end_date=None):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    mysqlCfg = envCfg.get('mysql_config', {})
    if not mysqlCfg:
        return None
    conn = MySQLdb.connect(host=mysqlCfg['host'],
                           user=mysqlCfg['user'],
                           passwd=urllib.quote(mysqlCfg['passwd']),
                           port=mysqlCfg['port'],
                           db=mysqlCfg['database'])
    conn.autocommit(True)
    cursor = conn.cursor()
    sqlCmd = '''
select
    url_sign,
    publish_time,
    source_name,
    related_sign,
    title,
    json_text
from
    tb_news
where
    (
        channel_id not in (10011, 10012)
    )
    and (is_visible = 1)
    and (
        date(
            from_unixtime(publish_time)
        ) BETWEEN '%s' and '%s'
    )
'''
    if not start_date:
        start_date = date(2015, 1, 1)
    if not end_date:
        end_date = date.today() + timedelta(days=1)
    startDateStr = start_date.strftime('%Y-%m-%d')
    endDateStr = end_date.strftime('%Y-%m-%d')
    cursor.execute(sqlCmd % (startDateStr, endDateStr))
    newsLst = []
    for idx, (newsId, publishTime, source_name, relatedSign, title, doc) in \
            enumerate(cursor.fetchall()):
        if idx % 100 == 0:
            print 'fetch %s news...' % idx
        newsLst.append((newsId, publishTime, source_name, relatedSign,
                        title.decode('utf-8'), doc.decode('utf-8')))
    return newsLst

def countHammingDis(hash1, hash2):
    if not hash1 or not hash2:
        #print 'simhash value don\'t exists!'
        return -1
    if len(hash1)!= len(hash2):
        #print "not allow different length"
        return -1
    length = len(hash1)
    dis = 0
    for i in range(length):
        if hash1[i]!=hash2[i]:
            dis+=1
    return dis

def findHotNews(newsLst):
    newsCount = len(newsLst)
    clusterLst = range(newsCount)
    for i in range(newsCount):
        for j in range(i+1, newsCount):
            news1 = newsLst[i]
            news2 = newsLst[j]
            dis = countHammingDis(news1[3], news2[3])
            if dis == -1:
                continue
            if len(news1[5])<=100 or len(news2[5])<=100:
                continue
            #duplicated and different source
            if dis<=MINTHRE_HAMMING_DISTANCE and news1[2]!=news2[2]:
                #merge two cluster
                cluster1 = clusterLst[i]
                cluster2 = clusterLst[j]
                clusterLst = merge(clusterLst, cluster1, cluster2)
    return clusterLst

def merge(clusterLst, cls1, cls2):
    minCls = min(cls1, cls2)
    newLst = []
    for item in clusterLst:
        if item==cls1 or item==cls2:
            newLst.append(minCls)
        else:
            newLst.append(item)
    return newLst

def checkCluster(clusterLst, newsLst):
    simDct = {}
    hotNews = []
    for idx, cluster in enumerate(clusterLst):
        if simDct.has_key(cluster):
            simDct[cluster].append((idx,newsLst[idx][0]))
        else:
            simDct[cluster] = [(idx, newsLst[idx][0])]
    for key in simDct:
        if len(simDct[key])>3:
            print simDct[key]
            print >>log_file, '*' * 40
            channelDct = {}
            for idx, newsId in simDct[key]:
                if not channelDct.has_key(newsLst[idx][2]):
                    channelDct[newsLst[idx][2]] = True
            if len(channelDct)>3:
                hotNews.append(simDct[key])
                for idx, newsId in simDct[key]:
                    print >>log_file, json.dumps(newsLst[idx], indent=1)
    return hotNews

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=2)
    newsLst = getSpanNews(start_date=start_date,
                             end_date=end_date)

    clusterLst = findHotNews(newsLst)
    hotNews = checkCluster(clusterLst, newsLst)

    print hotNews
