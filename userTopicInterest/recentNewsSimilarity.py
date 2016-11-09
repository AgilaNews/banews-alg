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

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)
KDIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = '/data/mysqlBackup/banews'
PREPROCESS_DATA_DIR = '/data/userTopicDis'
MODEL_DIR = os.path.join(PREPROCESS_DATA_DIR, 'model')
ATTRIBUTE_DIM = 9
(NEWS_ID, SRC_URL, CHANNEL_ID, TITLE, SRC_NAME, \
        PUBLISH_TIME, FETCH_TIME, CONTENT, TYPE) = \
        range(ATTRIBUTE_DIM)
N_TOPIC = 80
VALID_POS_LST = [NOUN, ADJ, NAMES]
en_nlp = spacy.load('en')

NEWS_TOPIC_SIMILARITY_KEY = 'NEWS_TOPIC_SIMILARITY_KEY'

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

def stemDoc(newsDoc):
    stemmedDocLst = []
    for token in newsDoc:
        if token.pos not in VALID_POS_LST:
            continue
        stemmedDocLst.append(token.lemma_)
    stemmedDocStr = ' '.join(stemmedDocLst)
    return stemmedDocStr

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

def predict(newsDocLst):
    print 'predict %s news...' % len(newsDocLst)
    with open(os.path.join(MODEL_DIR, 'ldaModel.m'), 'rb') as fp:
        ldaModel = pickle.load(fp)
    with open(os.path.join(MODEL_DIR, 'vectorizer.m'), 'rb') as fp:
        vectorizer = pickle.load(fp)
    stemmedDocLst = []
    (idLst, docLst, publishTimeLst) = zip(*newsDocLst)
    for idx, curDoc in enumerate(en_nlp.pipe(docLst,
            batch_size=50, n_threads=4)):
        if idx % 100 == 0:
            print 'preprocessing %s...' % idx
        stemmedDocStr = stemDoc(curDoc)
        stemmedDocLst.append(stemmedDocStr)
    tfMatrix = vectorizer.transform(stemmedDocLst)
    newsTopicArr = ldaModel.transform(tfMatrix)
    resLst = []
    for idx, topicArr in enumerate(newsTopicArr):
        newsId = idLst[idx]
        resLst.append((newsId, topicArr))
    return resLst


def dump(simTopicDct):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        return None
    redisCli = Redis(host=redisCfg['host'],
                     port=redisCfg['port'])
    # dump news topic similarity recently
    if redisCli.exists(NEWS_TOPIC_SIMILARITY_KEY):
        redisCli.delete(NEWS_TOPIC_SIMILARITY_KEY)
    tmpDct= {}
    for (key, value) in simTopicDct:
        if len(tmpDct) >=50:
            print 'dumping %s news topic similarity...' % len(tmpDct)
            redisCli.hmset(NEWS_TOPIC_SIMILARITY_KEY,
                            tmpDct)
            tmpDct = {}
        tmpDct[key] = value
    if len(tmpDct):
        print 'dumping %s news topic similarity...' % len(tmpDct)
        redisCli.hmset(NEWS_TOPIC_SIMILARITY_KEY,
                        tmpDct)

def calTopicSim(newsTopicLst):
    numNews = len(newsTopicLst)
    simTopicDct = {}
    for i in range(numNews):
        for j in range(i, numNews):
            newsIdx, topicLstx = newsTopicLst[i]
            newsIdy, topicLsty = newsTopicLst[j]
            cos_sim = cos_similarity(topicLstx, topicLsty)
            if not simTopicDct.get(newsIdx):
                simTopicDct[newsIdx] = {newsIdy:cos_sim}
            else:
                simTopicDct[newsIdx][newsIdy] = cos_sim
            if not simTopicDct.get(newsIdy):
                simTopicDct[newsIdy] = {newsIdx:cos_sim}
            else:
                simTopicDct[newsIdy][newsIdx] = cos_sim
                
    return simTopicDct

def cos_similarity(a, b):
    a = numpy.array(a)
    b = numpy.array(b)
    if len(a)*len(b)==0:
        return 0.0
    elif len(a)!=len(b):
        return 0.0
    elif a.dot(a)*b.dot(b)==0:
        return 0.0
    else:
        return a.dot(b)/(a.dot(a)*b.dot(b))**0.5

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=1)
    #select news(id, title, json_text, time) from mysql
    #set a timespan to calculate similarity
    newsDocLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    #get topic distribution of each news in newsDocLst(newsId, topicArr)
    newsTopicLst = predict(newsDocLst)
    #calculate topic similarity of recent news
    simTopicDct = calTopicSim(newsTopicLst)
    #dump news topic similarity to redis:NEWS_TOPIC_SIMILARITY_KEY
    dump(simTopicDct)
