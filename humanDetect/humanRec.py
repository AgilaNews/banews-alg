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
from unshortenit import unshorten
import requests
import logging
from time import sleep
import lxml
import lxml.html
from bs4 import UnicodeDammit
import re

import twitter
import facebook
from scrapyd_api import ScrapydAPI
from elasticsearch import Elasticsearch
from dateutil.parser import parse as dateParser

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
        'Url:{orgUrl}, code:{code}'
CRAWL_LOG_MSG = '[Crawling] Project:{project}, Spider:{spiderName}, ' \
        'JobId:{jobId}, NewsCnt:{newsCnt}, NewsSigns:{newsSigns}'
REDIS_LOG_MSG = '[Redising] NewsCnt:{newsCnt}, NewsSigns:{newsSigns}'
requests.packages.urllib3.disable_warnings()

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

def getEsClient():
    env = settings.CURRENT_ENVIRONMENT_TAG
    cfgDct = settings.ENVIRONMENT_CONFIG[env]
    es_servers = cfgDct['elastic_search_config']['servers']
    es_servers = es_servers if isinstance(es_servers, list) else [es_servers]
    es_timeout = cfgDct['elastic_search_config']['timeout']
    esClient = Elasticsearch(hosts=es_servers, timeout=es_timeout)
    return esClient

def getRedisClient():
    env = settings.CURRENT_ENVIRONMENT_TAG
    envCfg = settings.ENVIRONMENT_CONFIG.get(env, {})
    redisCfg = envCfg.get('news_queue_redis_config', {})
    if not redisCfg:
        logger.error('redis configuration not exist!')
        exit(1)
    redisCli = Redis(host=redisCfg['host'],
                     port=redisCfg['port'])
    return redisCli

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

def unshortenUrlV2(url, timeout):
    HTTP_HEADER = {
        "User-Agent": 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 ' \
                '(KHTML, like Gecko)  Chrome/47.0.2526.111 Safari/537.36',
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip,deflate,sdch",
        "Connection": "keep-alive",
        "Accept-Language": "nl-NL,nl;q=0.8,en-US;q=0.6,en;q=0.4",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }
    try:
        res = requests.head(url,
                          headers=HTTP_HEADER,
                          timeout=timeout,
                          allow_redirects=True,
                          verify=False)
        return (res.url, res.status_code)
    except:
        return (None, None)

def calculateSco(favoriteCnt, retweetCnt, createdTime):
    score = favoriteCnt + 3 * retweetCnt
    if createdTime >= NOW:
        span = 0
    else:
        span = (NOW - createdTime).total_seconds() / 3600
    if span < 48:
        score *= pow(0.5, span)
    else:
        score = -1
    return score

def parsePage(screenName, statusId):
    link = 'https://mobile.twitter.com/{screenName}/status/{statusId}'.format(
                screenName=screenName,
                statusId=statusId
            )
    r = requests.get(link)
    if r.status_code == 200:
        if isinstance(r.text, unicode):
            text = r.text
        else:
            converted = UnicodeDammit(r.text, is_html=True)
            text = converted.unicode_markup
        if text.startswith('<?'):
            text = re.sub(r'^\<\?.*?\?\>', '', text, flags=re.DOTALL)
        domEle = lxml.html.fromstring(text)
        resLst = domEle.xpath('//div[@data-id="%s"]//a[@title]/' \
                '@data-expanded-url'%statusId)
        if resLst:
            return resLst[0]
    return None

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
                orgUrl = urlObj.expanded_url
                #(cleUrl, code) = unshorten(orgUrl, timeout=10)
                (cleUrl, code) = unshortenUrlV2(orgUrl, timeout=10)
                if cleUrl and ('twitter.com' in cleUrl):
                    orgUrl = parsePage(screenName, statusId)
                    (cleUrl, code) = unshortenUrlV2(orgUrl, timeout=10)
                if ((code != 200) and (cleUrl == orgUrl)) or \
                        not cleUrl:
                    logger.error(STATUS_LOG_ERR.format(
                        media=media,
                        statusId=statusId,
                        orgUrl=orgUrl,
                        code=code))
                    continue
                if cleUrl:
                    #parsed = urlparse(cleUrl)
                    #cleUrl = '{uri.scheme}://{uri.netloc}{uri.path}'.format(
                    #        uri=parsed)
                    urlSign = create_sign(cleUrl)
                else:
                    urlSign = None
                favoriteCnt = statusObj.favorite_count
                retweetCnt = statusObj.retweet_count
                createdTime = dateParser(statusObj.created_at,
                        fuzzy=True)
                createdTime = createdTime.replace(tzinfo=None)
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
                    if score < 0:
                        continue
                    newsScoLst.append((urlSign, cleUrl, score))
    return newsScoLst

def crawlNews(scrapydCli, project, spider, newsScoLst):
    if newsScoLst:
        urlSigns = ','.join(map(lambda val: val[0], newsScoLst))
        links = ','.join(map(lambda val: val[1], newsScoLst))
        jobId = scrapydCli.schedule(project, spider,
                links=links)
        logger.info(CRAWL_LOG_MSG.format(
            project=project,
            spiderName=spider,
            jobId=jobId,
            newsCnt=len(newsScoLst),
            newsSigns=urlSigns))

def dumpRedis(newsScoLst):
    if not newsScoLst:
        return None
    esCli = getEsClient()
    env = settings.CURRENT_ENVIRONMENT_TAG
    cfgDct = settings.ENVIRONMENT_CONFIG[env]
    indexKey = cfgDct['elastic_search_config']['index']
    typeKey = cfgDct['elastic_search_config']['type']
    filterNewsLst = []
    for newsId, sco in newsScoLst:
        try:
            resDct = esCli.get(index=indexKey,
                            doc_type=typeKey,
                            id=newsId)
            if resDct['_source'].get('plain_text') and \
                    resDct['_source'].get('title') and \
                    resDct['_source'].get('post_timestamp'):
                filterNewsLst.append((newsId, sco))
        except:
            continue
    redisCli = getRedisClient()
    if redisCli.exists(ALG_EDITOR_REC_KEY):
        redisCli.delete(ALG_EDITOR_REC_KEY)
    totalCnt = 0
    tmpDct = {}
    for idx, (newsId, score) in enumerate(filterNewsLst):
        totalCnt += 1
        if len(tmpDct) >= 10:
            redisCli.zadd(ALG_EDITOR_REC_KEY, **tmpDct)
            logger.info(REDIS_LOG_MSG.format(
                newsCnt=len(tmpDct),
                newsSigns=','.join(tmpDct.keys())))
            tmpDct = {}
        tmpDct[newsId] = score
    if len(tmpDct):
        logger.info(REDIS_LOG_MSG.format(
            newsCnt=len(tmpDct),
            newsSigns=','.join(tmpDct.keys())))
        redisCli.zadd(ALG_EDITOR_REC_KEY, **tmpDct)

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
    logger.info('[Sleeping] start sleeping 10 mins...')
    sleep(60 * 10)
    dumpRedis(mergeNewsScoLst)
    logger.info('[Finish] dumping data to redis successfully')

if __name__ == '__main__':
    main(TWITTER)
