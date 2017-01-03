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

import pickle
import spacy
from spacy.parts_of_speech import (NOUN, ADJ, NAMES)
import settings
from simhash import simhash

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
KDIR = os.path.dirname(os.path.abspath(__file__))
PREPROCESS_DATA_DIR = '/data/userTopicDis'
MODEL_DIR = os.path.join(PREPROCESS_DATA_DIR, 'model')
ATTRIBUTE_DIM = 9
(NEWS_ID, SRC_URL, CHANNEL_ID, TITLE, SRC_NAME, \
        PUBLISH_TIME, FETCH_TIME, CONTENT, TYPE) = \
        range(ATTRIBUTE_DIM)
N_TOPIC = 80
VALID_POS_LST = [NOUN, ADJ, NAMES]
en_nlp = spacy.load('en')

SIMHASH_LENGTH = 128

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

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
    title,
    json_text,
    publish_time
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
    newsDocLst = []
    for idx, (newsId, titleStr, docStr, publishTime) in \
            enumerate(cursor.fetchall()):
        if idx % 100 == 0:
            print 'fetch %s news...' % idx
        textStr = titleStr + ' ' + stripTag(docStr)
        newsDocLst.append((newsId,
                           textStr.decode('utf-8'),
                           publishTime))
    return newsDocLst

def updateSimhash(newsLst):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    mysqlCfg = envCfg.get('mysql_config', {})
    print mysqlCfg
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
update tb_news
    set related_sign = '%s'
where
    url_sign = '%s'
'''
    for idx, (newsId, related_sign) in enumerate(newsLst):
        if idx % 100 == 0:
            print 'update %s news: %s, %s...' % (idx, newsId, related_sign) 
        cursor.execute(sqlCmd % (related_sign, newsId))

def stemDoc(newsDoc):
    stemmedDocLst = []
    for token in newsDoc:
        if token.pos not in VALID_POS_LST:
            continue
        stemmedDocLst.append(token.lemma_)
    stemmedDocStr = ' '.join(stemmedDocLst)
    return stemmedDocStr

def predict(newsDocLst):
    print 'calculating simhash value of %s news...' % len(newsDocLst)
    simhashLst = []
    (idLst, docLst, publishTimeLst) = zip(*newsDocLst)
    for idx, curDoc in enumerate(en_nlp.pipe(docLst,
            batch_size=50, n_threads=4)):
        if idx % 100 == 0:
            print 'preprocessing %s...' % idx
        #calculate simhash value
        stemmedDocStr = stemDoc(curDoc)
        hashValue = simhash(stemmedDocStr.lower().split()).hash
        #decimal to binary
        hashValue = bin(hashValue)[2:]
        if len(hashValue)<SIMHASH_LENGTH:
            hashValue = '0' * (SIMHASH_LENGTH-len(hashValue)) + hashValue
        simhashLst.append(hashValue)
    return zip(idLst, simhashLst)

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=10)
    #select news(id, title, json_text, time) from mysql
    newsDocLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    #calculating simhash value of news 
    newsSimhashLst = predict(newsDocLst)
    #update mysql
    updateSimhash(newsSimhashLst)
