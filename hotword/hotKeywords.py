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

from rake import Rake
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
    tag
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
    for idx, (newsId, title, doc, tags) in \
            enumerate(cursor.fetchall()):
        if idx % 100 == 0:
            print 'fetch %s news...' % idx
        newsLst.append((newsId,title.decode('utf-8'), 
                        doc.decode('utf-8'), tags.decode('utf-8')))
    return newsLst

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

if __name__ == '__main__':
    end_date = datetime.now()
    start_date = datetime.now() - timedelta(hours=4)
    newsLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    print len(newsLst), 'news fetched from database...'

    rake = Rake("SmartStoplist.txt")
    tagDct = {}
    
    for news in newsLst:
        tags = news[3]
        textStr = news[1]+' '+ news[1] + ' '+ news[2]
        tagLst = rake.run(stripTag(textStr))
        for tag in tagLst:
            if tagDct.has_key(tag):
                tagDct[tag] = tagDct[tag]+1
            else:
                tagDct[tag] = 1
    hotTagLst = sorted(tagDct.items(), key=lambda d:d[1], reverse=True)
    print hotTagLst[:50]


