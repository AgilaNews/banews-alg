# -*- coding:utf-8 -*-
import os
import re
import sys
import json
import math
import numpy
import MySQLdb
import urllib

from rake import Rake
from redis import Redis
from optparse import OptionParser
from datetime import date, datetime, timedelta
import settings

default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

ALG_HOT_KEYWORDS_KEY = 'ALG_HOT_KEYWORDS_KEY'

BLACK_WORD_LST = ['daily inquirer', 'gmanews', 'manila times online',
                  'news portal', 'rappler', 'tmz', 'article originally',
                  'abs-cbn',]
WORD_WRAP_DCT = {
        'nba':'NBA', 
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
ABBR_WRAP_DCT = {
    'president ': '',
    'united states': 'US.',
    'united kingdom': 'UK.',
    'united nation': 'UN',
    'philippine ': 'PH. ',
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
    # filter tag according to their source
    newsDct = {}
    resTagLst = []
    for tag, newsLst in tagLst:
        flag = False
        for news in newsLst:
            if newsDct.get(news, 0)>=2:
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

def filterDuplicateWord(tagLst):
    # filter tag according to their duplicated words
    # e.g. NBA basketball game, 2017 NBA season
    # 1.form word diction with word of tag
    wordDct = {}
    resTagLst = []
    for tag in tagLst:
        flag = True
        wordLst = tag.strip().split(' ')
        for word in wordLst:
            if wordDct.has_key(word):
                flag = False
                break
        if flag:
            resTagLst.append(tag)
            for word in wordLst:
                wordDct[word] = 1
    return resTagLst

def recentKeywords(newsLst, count=10):
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
    hotTagLst = filterDuplicateWord(hotTagLst)
    keywordLst =  filter(filterBlackWord, hotTagLst)
    keywordLst = wrapKeyword(keywordLst)[:count]
    return keywordLst

def filterBlackWord(tag):
    if len(tag)<5:
        return False
    if len(tag.strip().split(' '))>3:
        return False
    for word in BLACK_WORD_LST:
        if word in tag:
            return False
    return True

def wrapKeyword(tagLst):
    kwLst = []
    for tag in tagLst:
        for abbr in ABBR_WRAP_DCT:
            tag = tag.replace(abbr, ABBR_WRAP_DCT[abbr])
        wrapLst = []
        for word in tag.split(' '):
            if WORD_WRAP_DCT.has_key(word):
                wrapLst.append(WORD_WRAP_DCT[word])
                continue
            if word.islower():
                word = word.capitalize()
            wrapLst.append(word)
        kwLst.append(' '.join(wrapLst))
    return kwLst

def dump(wordLst):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        print 'redis configuration not exist!'
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])

    print 'REDIS HOST=%s, UPDATING REDIS...' %redisCfg['host']

    if redisCli.exists(ALG_HOT_KEYWORDS_KEY):
        redisCli.delete(ALG_HOT_KEYWORDS_KEY)
    print 'NEW ALG_HOT_KEYWORDS_KEY', wordLst
    tmpDct = {}
    for idx, word in enumerate(wordLst):
        tmpDct[idx] = word
    redisCli.hmset(ALG_HOT_KEYWORDS_KEY, tmpDct)
    print 'SUCCESS: UPDATE REDIS ALG_HOT_KEYWORDS_KEY'

def updateKeyword(idx, word):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        print 'redis configuration not exist!'
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])

    print 'REDIS HOST=%s, UPDATING REDIS...' %redisCfg['host']

    if redisCli.exists(ALG_HOT_KEYWORDS_KEY):
        redisCli.delete(ALG_HOT_KEYWORDS_KEY)
    print 'NEW ALG_HOT_KEYWORDS_KEY', idx, word
    redisCli.hset(ALG_HOT_KEYWORDS_KEY, idx, word)
    print 'SUCCESS: UPDATE REDIS ALG_HOT_KEYWORDS_KEY'

def display(count=10):
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        print 'redis configuration not exist!'
    redisCli = Redis(host=redisCfg['host'], port=redisCfg['port'])
    kwDct = redisCli.hscan(ALG_HOT_KEYWORDS_KEY)
    if not kwDct:
        print 'no value stored in the key'
    else:
        print kwDct
    return kwDct


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-a', '--action', dest='action', default='extract')
    parser.add_option('-t', '--times', dest='delta', default=4)
    parser.add_option('-c', '--count', dest='count', default=20)
    parser.add_option('-i', '--idx', dest='idx', default=-1)
    parser.add_option('-w', '--word', dest='newWord', default='')
    parser.add_option('-s', '--sort', dest='isSorted', default=False)
    (options, args) = parser.parse_args()
    if options.action == 'extract':
        # update keywords
        end_date = datetime.now()
        start_date = datetime.now() - timedelta(hours=options.delta)
        newsLst = getSpanNews(start_date=start_date,
                                 end_date=end_date)
        keywordLst = recentKeywords(newsLst, options.count)
        if options.isSorted == True:
            keywordLst = sorted(keywordLst, key=lambda d:len(d))
        dump(keywordLst)
    elif options.action == 'show':
        # show all keywords in redis key
        kwDct = display(options.count)
    elif options.action == 'update':
        if options.idx < 0:
            print 'function---dump(kwLst): update all keyword list with a new list'
            print 'function---updateKeyword(idx, newWord): replace keyword at key idx with new word'
        else:
            updateKeyword(options.idx, options.newWord)

        

