# -*- coding:utf-8 -*-
import os
import re
from datetime import date, datetime, timedelta
import json
import math
import urllib
import MySQLdb
from redis import Redis
from operator import add
try:
    from pyspark import SparkContext
except:
    pass

import settings
from rake import Rake

TODAY_LOG_DIR = '/banews/useraction.log-*'
HISTORY_LOG_DIR = '/banews/useraction'

HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ARTICLE_DISPLAY = '020104'
ARTICLE_CLICK = '020103'
ARTICLE_LIKE = '020204'
ARTICLE_COLLECT = '020205'
GRAVITY = 20.
NEWS_TOPICS_PATH = '/data/userTopicDis/model/newsTopic.d'
MIN_NEWS_TOPIC_WEIGHT = 0.1
MIN_USER_ACTION_CNT = 20
USER_TOPIC_INTEREST_DIR = '/user/limeng/userTopicInterest'
ALG_USER_TOPIC_KEY = 'ALG_USER_TOPIC_KEY'
ALG_TOPIC_GRAVITY_KEY = 'ALG_TOPIC_GRAVITY_KEY'


def getSpanFileLst(start_date, end_date, withToday=False):
    fileLst = []
    if start_date >= end_date:
        return fileLst
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
        withToday = True
    if withToday:
        res = os.popen('%s fs -ls %s' % \
                (HADOOP_BIN, TODAY_LOG_DIR)).readlines()
        if len(res):
            fileLst.append(TODAY_LOG_DIR)

    # append history's log
    cur_date = start_date
    while cur_date < end_date:
        if cur_date >= date.today():
            cur_date += timedelta(days=1)
            continue
        curDir = os.path.join(HISTORY_LOG_DIR,
                              cur_date.strftime('%Y/%m/%d'),
                              'useraction.log-*')
        res = os.popen("%s fs -ls %s" % (HADOOP_BIN, curDir)).readlines()
        if len(res) != 0:
            fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getActionLog(sc, start_date, end_date):
    # but can exist in mulitple topics
    eventIdLst = [ARTICLE_CLICK, ARTICLE_LIKE, ARTICLE_COLLECT]
    fileLst = getSpanFileLst(start_date, end_date)
    logRdd = sc.textFile(','.join(fileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: (attrDct.get('event-id') in eventIdLst) and \
                                ('did' in attrDct) and \
                                ('news_id' in attrDct)
            ).map(
                # userId, topicIdx, newsId, eventId, timestamp
                lambda attrDct: (str(attrDct.get('news_id')), 1)
            ).reduceByKey(add)
    return logRdd

def getNewsById(newsId):
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
    title,
    json_text,
    tag
from
    tb_news
where
    url_sign = '%s'
'''
    cursor.execute(sqlCmd % (newsId))
    for idx, (newsId, title, doc, tags) in \
            enumerate(cursor.fetchall()):
        return (newsId, title.decode('utf-8'), 
                doc.decode('utf-8'), tags.decode('utf-8'))

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)
    
if __name__ == '__main__':
    sc = SparkContext(appName='newsTrend/limeng')
    end_date = date.today()
    start_date = end_date - timedelta(days=1)
    logRdd = getActionLog(sc, start_date, end_date)
    hotNewsLst = sorted(logRdd.collect(), key=lambda d:d[1], reverse=True)[:100]
    print hotNewsLst
    rake = Rake("SmartStoplist.txt")
    tagDct = {}

    for newsItem in hotNewsLst:
        newsId = newsItem[0]
        newsClick = newsItem[1]

        news = getNewsById(newsId)
        textStr = news[1]+' '+news[1]+' '+news[2]
        tagLst = rake.run(stripTag(textStr))
        for tag in tagLst:
            if tagDct.has_key(tag):
                tagDct[tag] = tagDct[tag] + math.log(newsClick)
            else:
                tagDct[tag] = newsClick + math.log(newsClick)
    hotTagLst = sorted(tagDct.items(), key=lambda d:d[1], reverse=True)
    for i in range(30):
        print hotTagLst[i]


