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

MINTHRE_HAMMING_DISTANCE = 7

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
    simDct = {}
    newsCount = len(newsLst)
    for i in range(newsCount):
        for j in range(i+1, newsCount):
            news1 = newsLst[i]
            news2 = newsLst[j]
            dis = countHammingDis(news1[3], news2[3])
            if dis == -1:
                continue
            #print 'get hamming distance...', dis
            if dis<=MINTHRE_HAMMING_DISTANCE and \
                    news1[2]!=news2[2]:
                if simDct.has_key(news1[0]):
                    simDct[news1[0]].append((j,news2[0]))
                elif simDct.has_key(news2[0]):
                    simDct[news2[0]].append((i,news1[0]))
                else:
                    simDct[news1[0]] = [(j,news2[0])]
    return simDct


def printDct(newsDct, newsLst):
    for news in newsDct:
        if len(newsDct[news])>10:
            print '*'*40
            print news, newsDct[news]
            for idx, newsId in newsDct[news]:
                print '&&&',
                print newsId+','+newsLst[idx][4]+','+newsLst[idx][5][:50]

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=1)
    newsLst = getSpanNews(start_date=start_date,
                             end_date=end_date)

    hotNews = findHotNews(newsLst)
    printDct(hotNews, newsLst)
