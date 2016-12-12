# -*- coding:utf-8 -*-
import os
import sys
sys.path.insert(0, '/home/work/spider/newsCrawler/newsCrawler')
from datetime import datetime
from ConfigParser import ConfigParser
from random import sample
from redis import Redis
from math import pow
from urlparse import urlparse
from httplib import HTTPConnection
import requests
import logging

import twitter
import facebook
from scrapyd_api import ScrapydAPI

import settings
from utils.signature import create_sign

formatter = logging.Formatter(
        '%(levelname)s [%(name)s] %(asctime)s: %(message)s')
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(formatter)
logger = logging.getLogger('humanRec')
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
SCREEN_LOG_MSG = '[Tracking] Media:{media} Spider:{spiderName}, ' \
        'ScreenName:{screenName}'
STATUS_LOG_MSG = '[Posting] Media:{media}, StatusId:{statusId}, ' \
        'Favor:{favorCnt}, Tweeted:{tweetCnt}, OriginalUrl:{orgUrl}, ' \
        'Url:{cleUrl}, UrlSign:{urlSign}'
STATUS_LOG_ERR = '[Posting] Media:{media}, StatusId:{statusId}, ' \
        'Url:{orgUrl}, Depth:{depth}, Error:{error}'
CRAWL_LOG_MSG = '[Crawling] Project:{project}, Spider:{spiderName}, ' \
        'JobId:{jobId}, NewsCnt:{newsCnt}, NewsSigns:{newsSigns}'
REDIS_LOG_MSG = '[Redising] NewsCnt:{newsCnt}, NewsSigns:{newsSigns}'

kDIR = os.path.dirname(os.path.abspath(__file__))
(TWITTER, FACEBOOK) = ('Twitter', 'Facebook')
TWITTER_ACCOUNT_KEY = 'SPIDER_TWITTER'
FACEBOOK_ACCOUNT_KEY = 'SPIDER_FACEBOOK'
MAX_DEPTH = 5
NOW = datetime.now()
ALG_EDITOR_REC_KEY = 'ALG_EDITOR_REC_KEY'

def getScrapydClient():
    scrapydUrl = settings.SCRAPYD_URL
    scrapydCli = ScrapydAPI(scrapydUrl)
    return scrapydCli

def getApi(parser, media=TWITTER):
    parser.optionxform = str
    parser.read(os.path.join(kDIR, 'token.cfg'))
    sectionLst = [section for section in parser.sections() \
            if section.startswith(media)]
    selectSec = ''
    if len(sectionLst) > 0:
        selectSec = sample(sectionLst, 1)[0]
        if media == TWITTER:
            consumer_key = parser.get(selectSec, 'consumer_key')
            consumer_secret = parser.get(selectSec, 'consumer_secret')
            access_key = parser.get(selectSec, 'access_key')
            access_secret = parser.get(selectSec, 'access_secret')
            api = twitter.Api(consumer_key=consumer_key,
                              consumer_secret=consumer_secret,
                              access_token_key=access_key,
                              access_token_secret=access_secret,
                              input_encoding='utf-8')
        elif media == FACEBOOK:
            access_token = parser.get(selectSec, 'access_token')
            api = facebook.GraphAPI(access_token)
        return api
    return None

def getAccountLst(parser, media=TWITTER):
    if media == TWITTER:
        accountLst = parser.items(TWITTER_ACCOUNT_KEY)
    elif media == FACEBOOK:
        accountLst = parser.items(FACEBOOK_ACCOUNT_KEY)
    else:
        print 'Media %s not exist, Error!'
        exit(0)
    return accountLst

def unshortenUrl(media, statusId, url, depth):
    '''
    linke: http://stackoverflow.com/questions/7153096/how-can-i-un-shorten-a-url-using-python/7153185#7153185
    '''
    if depth > MAX_DEPTH:
        return None
    parsed = urlparse(url)
    try:
        h = HTTPConnection(parsed.netloc, timeout=20)
        resource = parsed.path
        if parsed.query != '':
            resource += '?' + parsed.query
        h.request('HEAD', resource)
        response = h.getresponse()
        if (response.status/100 == 3) and response.getheader('Location'):
            return unshortenUrl(media, statusId,
                    response.getheader('Location'), depth+1)
        else:
            cleanUrl = '{uri.scheme}://{uri.netloc}{uri.path}'.format(
                    uri=parsed)
            return (url, cleanUrl)
    except Exception as err:
        logger.error(STATUS_LOG_ERR.format(
            media=media,
            statusId=statusId,
            orgUrl=url,
            depth=depth,
            error=err))
        return None

def calculateSco(favoriteCnt, retweetCnt, createdTime):
    score = favoriteCnt + 3 * retweetCnt
    if createdTime >= NOW:
        span = 0
    else:
        span = (NOW - createdTime).total_seconds() / 60
    score *= pow(0.5, span)
    return score

def getUserTweets(media, api, spiderName, screenName, count=50):
    statusLst = api.GetUserTimeline(screen_name=screenName,
                                    exclude_replies=False,
                                    count=count)
    newsScoLst = []
    alreadyNewsSet = set()
    for idx, statusObj in enumerate(statusLst):
        if statusObj.urls:
            statusId = statusObj.id
            for urlObj in statusObj.urls:
                if not urlObj.expanded_url:
                    continue
                resLst = unshortenUrl(media, statusId,
                        urlObj.expanded_url, 0)
                if not resLst:
                    continue
                (orgUrl, cleUrl) = resLst
                if cleUrl:
                    urlSign = create_sign(cleUrl)
                else:
                    urlSign = None
                favoriteCnt = statusObj.favorite_count
                retweetCnt = statusObj.retweet_count
                createdTime = statusObj.created_at
                createdTime = ' '.join(createdTime.strip().split()[:4])
                createdTime = datetime.strptime(createdTime,
                        '%a %b %y %H:%M:%S')
                logger.info(STATUS_LOG_MSG.format(
                    media=media,
                    statusId=statusId,
                    favorCnt=favoriteCnt,
                    tweetCnt=retweetCnt,
                    orgUrl=orgUrl,
                    cleUrl=cleUrl,
                    urlSign=urlSign))
                if urlSign and (urlSign not in alreadyNewsSet):
                    alreadyNewsSet.add(urlSign)
                    score = calculateSco(favoriteCnt, retweetCnt,
                            createdTime)
                    newsScoLst.append((urlSign, cleUrl, score))
    return newsScoLst

def crawlNews(scrapydCli, project, spider, newsScoLst,
        bulkSize=10):
    bulkCnt = len(newsScoLst) / bulkSize + 1
    for idx in range(bulkCnt):
        startIdx = idx * bulkSize
        endIdx = (idx + 1) * bulkSize
        curUrlLst = newsScoLst[startIdx:endIdx]
        if not curUrlLst:
            continue
        urlSigns = ','.join(map(lambda val: val[0], curUrlLst))
        links = ','.join(map(lambda val: val[1], curUrlLst))
        jobId = scrapydCli.schedule(project, spider,
                links=links)
        logger.info(CRAWL_LOG_MSG.format(
            project=project,
            spiderName=spider,
            jobId=jobId,
            newsCnt=len(curUrlLst),
            newsSigns=urlSigns))

def dumpRedis(newsScoLst):
    if not newsScoLst:
        return None
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        logger.error('redis configuration not exist!')
        exit(1)
    redisCli = Redis(host=redisCfg['host'],
                     port=redisCfg['port'])
    if redisCli.exists(ALG_EDITOR_REC_KEY):
        redisCli.delete(ALG_EDITOR_REC_KEY)
    totalCnt = 0
    tmpDct = {}
    for idx, (newsId, score) in enumerate(newsScoLst):
        totalCnt += 1
        if len(tmpDct) >= 10:
            redisCli.hmset(ALG_EDITOR_REC_KEY, tmpDct)
            logger.info(REDIS_LOG_MSG.format(
                newsCnt=len(tmpDct),
                newsSigns=','.join(tmpDct.keys())))
            tmpDct = {}
        tmpDct[newsId] = score
    if len(tmpDct):
        logger.info(REDIS_LOG_MSG.format(
            newsCnt=len(tmpDct),
            newsSigns=','.join(tmpDct.keys())))
        redisCli.hmset(ALG_EDITOR_REC_KEY, tmpDct)

def main(media, project=settings.BOT_NAME):
    parser = ConfigParser()
    # track news feed list from twitter & facebook
    api = getApi(parser, media=media)
    accountLst = getAccountLst(parser, media=media)
    spiderNewsLst = []
    for idx, (spiderName, screenName) in enumerate(accountLst):
        logger.info(SCREEN_LOG_MSG.format(
            media=media,
            spiderName=spiderName,
            screenName=screenName))
        newsScoLst = getUserTweets(media, api, spiderName,
                screenName)
        spiderNewsLst.append((spiderName, newsScoLst))
    # crawl news from according source
    scrapydCli = getScrapydClient()
    mergeNewsScoLst = []
    for idx, (spiderName, newsScoLst) in enumerate(spiderNewsLst):
        crawlNews(scrapydCli, project, spiderName, newsScoLst)
        for urlSign, cleUrl, score in newsScoLst:
            mergeNewsScoLst.append((urlSign, score))
    # dump news score information to redis
    dumpRedis(mergeNewsScoLst)

if __name__ == '__main__':
    main(TWITTER)
