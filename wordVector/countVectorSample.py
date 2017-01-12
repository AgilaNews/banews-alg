# -*- coding:utf-8 -*-
import os
import re
import sys
from datetime import date, datetime, timedelta
import json
import math
import urllib
import MySQLdb
from redis import Redis
import settings
from sklearn.feature_extraction.text import TfidfVectorizer

MAX_COUNT_FEATURES = 50000
WORD_IDF_PATH = '/data/models/idf.d'

reload(sys)
sys.setdefaultencoding("utf-8")

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
        newsDocLst.append(textStr.decode('utf-8'))
    return newsDocLst

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

def dump(idfLst):
    with open(WORD_IDF_PATH, 'w') as f:
        for item in idfLst:
            f.write(item[0]+'\t'+str(item[1])+'\n')
    print 'succesfully update word IDF'

if __name__ == '__main__':
    end_date = date.today() + timedelta(days=1)
    start_date = date.today() - timedelta(days=20)
    newsDocLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    tfidf_vector = TfidfVectorizer(max_df=0.95, min_df=2,
                                   max_features=MAX_COUNT_FEATURES,
                                   stop_words='english') 
    tfidf_vector.fit_transform(newsDocLst)
    feature_names = tfidf_vector.get_feature_names()
    print len(feature_names), len(tfidf_vector.idf_)
    idfDct =  dict(zip(feature_names, tfidf_vector.idf_))
    sortedIDFLst = sorted(idfDct.items(), key=lambda x:x[1], reverse=True)
    print '*'*30
    for item in sortedIDFLst:
        print item, 
    print '*'*30
    dump(sortedIDFLst)

