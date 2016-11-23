# -*- coding:utf-8 -*-
import os
import re
import sys
import json
import numpy
from datetime import date, datetime, timedelta
import urllib
from redis import Redis

import pickle
import settings

try:
    from pyspark import SparkContext
except:
    pass

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

NEWS_TOPIC_SIMILARITY_DIR = '/data/calcSim/output'
ALG_TOPIC_SIMILARITY_KEY = 'ALG_TOPIC_SIMILARITY_KEY'


def dump(sc, newsSimStr):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        print 'redis configuration not exist!'
        exit(0)
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])

    if redisCli.exists(ALG_TOPIC_SIMILARITY_KEY):
        redisCli.delete(ALG_TOPIC_SIMILARITY_KEY)
    totalCnt = 0
    tmpDct = {}
    for line in newsSimStr:
        totalCnt += 1
        if len(tmpDct) >= 20:
            print '%s remaining...' % (len(newsSimStr) - totalCnt)
            redisCli.hmset(ALG_TOPIC_SIMILARITY_KEY, tmpDct)
            tmpDct = {}
        [newsId, simNews] = line.strip().split('\t')
        simNewsDct = {}
        for item in simNews.split(';'):
            [newsId2, newsWeight] = item.split(':')
            simNewsDct[newsId2] = float(newsWeight)
        tmpDct[newsId] = json.dumps(simNewsDct)
    if len(tmpDct):
        redisCli.hmset(ALG_TOPIC_SIMILARITY_KEY, tmpDct)

if __name__ == '__main__':
    sc = SparkContext(appName='newsTopic/limeng')
    newsSimilarity = sc.textFile(NEWS_TOPIC_SIMILARITY_DIR).collect()
    #dump news topic similarity to hadoop file
    dump(sc, newsSimilarity)
