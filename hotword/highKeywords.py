# -*- coding:utf-8 -*-
import os
import re
import sys
import json
import math
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

BLACK_WORD_LST = ['daily inquirer', 'gmanews', 'manila times online',
                    ]
WORD_WRAP_DCT = {'nba':'NBA', 
                    'i':'I',
                    'ii':'II',
                    'iii':'III',
                    'iv':'IV',
                    'v':'V',
                    'vi':'VI',
                    'vii':'VII',
                    'viii':'VIII',
                    'ix':'IX',
                    'x':'X',
                    }


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

def filterDuplicateNews(tagLst):
    # tagLst = {tag:[n1,n2], tag2:[n2,n3]...}
    newsDct = {}
    resTagLst = []
    for tag, newsLst in tagLst:
        flag = False
        for news in newsLst:
            if newsDct.get(news, 0)>2:
                flag = True
                break
        if flag==True:
            continue
        for news in newsLst:
            if not newsDct.has_key(news):
                newsDct[news] = 1
            else:
                newsDct[news] = newsDct[news]+1
        resTagLst.append(tag)
    return resTagLst

def recentKeywords(newsLst):
    rake = Rake("SmartStoplist.txt")
    tagDct = {}
    tagScoreDct = {}
    for news in newsLst:
        newsId = news[0]
        tags = news[3]
        textStr = (news[1]+' ')*3 + news[2]
        tagLst = rake.run(stripTag(textStr))
        for tag, score in tagLst:
            score = math.log(score, 2)
            if tagDct.has_key(tag):
                tagDct[tag].append(newsId)
            else:
                tagDct[tag] = [newsId,]
            tagScoreDct[tag] = tagScoreDct.get(tag, 0.0) + score

    hotTagLst = sorted(tagDct.items(), key=lambda d:tagScoreDct[d[0]], reverse=True)
    hotTagLst = filterDuplicateNews(hotTagLst)
    filterTagLst =  filter(filterBlackWord, hotTagLst)
    return filterTagLst

def filterBlackWord(tag):
    if len(tag)<5:
        return False
    for word in BLACK_WORD_LST:
        if word in tag:
            return False
    return True

def wrapKeyword(tagLst):
    kwLst = []
    for tag in tagLst:
        wrapLst = []
        for word in tag.split(' '):
            if WORD_WRAP_DCT.has_key(word):
                wrapLst.append(WORD_WRAP_DCT[word])
                continue
            wrapLst.append(word.capitalize())
        kwLst.append(' '.join(wrapLst))
    return kwLst

if __name__ == '__main__':
    end_date = datetime.now()
    start_date = datetime.now() - timedelta(hours=8)
    #fetch news content from db
    newsLst = getSpanNews(start_date=start_date,
                             end_date=end_date)
    print len(newsLst), 'news fetched from database...'
    #extract keyword from newsLst
    keywordLst = recentKeywords(newsLst)

    keywordLst = wrapKeyword(keywordLst)

    for item in keywordLst[:30]:
        print item+'\t',
    print '\n'
