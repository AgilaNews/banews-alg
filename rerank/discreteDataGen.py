# -*- coding:utf-8 -*-

import sys
import os
import re
from datetime import date, datetime, timedelta
from random import random
import json
import hashlib
import MySQLdb
import string
from optparse import OptionParser

from pyspark import SparkContext

import settings

KDIR = os.path.dirname(os.path.abspath(__file__))
ACTION_TODAY_LOG_DIR = '/banews/useraction.log-*'
ACTION_HISTORY_LOG_DIR = '/banews/useraction'
FEATURE_TODAY_LOG_DIR = '/banews/samplefeature/samplefeature.log-*'
FEATURE_HISTORY_LOG_DIR = '/banews/samplefeature'
HADOOP_BIN = '/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop'
ARTICLE_CLICK_EVENTID = '020103'
ARTICLE_DISPLAY_EVENTID = '020104'
ARTICLE_LIKE_EVENTID = '020204'
ARTICLE_COMMENT_EVENTID = '020207'
ACTION_WEGITH_DCT = {
            ARTICLE_CLICK_EVENTID: 1,
            ARTICLE_LIKE_EVENTID: 3,
            ARTICLE_COMMENT_EVENTID: 3
        }
MIN_USER_ACTION = 5
ROUND_CNT = 5
POSITIVE_TAG = '1'
NEGATIVE_TAG = '-1'
FEATURE_MAP_NAME = 'feature_mapping.listPage'
MIN_FEATURE_VALUE = 0.001
TOPIC_CNT = 80
TOPIC_PREFIX_KEY = 'TOPIC_%s'
TRAINING_DATA_HDFS = '/user/limeng/models/liblinear/trainingData'
TMP_DATA_PATH = '/data/tmp/alg'
FEATURE_SPACE_SIZE = 1000000
FEATURE_GAP = '_'

def getSpanFileLst(kind, start_date, end_date, withToday=False):
    if kind == 'useraction':
        todayLogDir = ACTION_TODAY_LOG_DIR
        historyLogDir = ACTION_HISTORY_LOG_DIR
    elif kind == 'samplefeature':
        todayLogDir = FEATURE_TODAY_LOG_DIR
        historyLogDir = FEATURE_HISTORY_LOG_DIR
    else:
        print 'unkown kind input, error!'
        exit(1)
    fileLst = []
    if start_date >= end_date:
        return fileLst
    # append today's log
    if (end_date > date.today()) or (start_date == date.today()):
        withToday = True
    if withToday:
        res = os.popen('%s fs -ls %s' % \
                (HADOOP_BIN, todayLogDir)).readlines()
        if len(res):
            fileLst.append(todayLogDir)

    # append history's log
    cur_date = start_date
    while cur_date < end_date:
        if cur_date >= date.today():
            cur_date += timedelta(days=1)
            continue
        curDir = os.path.join(historyLogDir,
                              cur_date.strftime('%Y/%m/%d'),
                              '*.log-*')
        res = os.popen("%s fs -ls %s" % (HADOOP_BIN, curDir)).readlines()
        if len(res) != 0:
            fileLst.append(curDir)
        cur_date += timedelta(days=1)
    return fileLst

def getUserActionLog(sc, start_date, end_date):
    userActFileLst = getSpanFileLst('useraction', start_date, end_date)
    actionLogRdd = sc.textFile(','.join(userActFileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: ('did' in attrDct) and \
                                ('session' in attrDct) and \
                                ('time' in attrDct) and \
                                (attrDct.get('event-id') in ACTION_WEGITH_DCT)
            ).map(
                lambda attrDct: (attrDct['did'],
                                 attrDct['session'],
                                 attrDct['event-id'],
                                 attrDct['news_id'])
            ).map(
                lambda (did, sessionId, eventId, newsId): \
                        ((did, newsId), ACTION_WEGITH_DCT[eventId])
            ).reduceByKey(
                lambda x, y: max(x, y)
            )
    return actionLogRdd

def getNewsExtra(start_date, end_date):
    env = settings.CURRENT_ENVIRONMENT_TAG
    mySqlCfgDct = settings.ENVIRONMENT_CONFIG[env]['mysql_config']
    conn = MySQLdb.connect(host=mySqlCfgDct['host'],
                           port=mySqlCfgDct['port'],
                           user=mySqlCfgDct['user'],
                           passwd=mySqlCfgDct['passwd'],
                           db=mySqlCfgDct['database'])
    conn.autocommit(True)
    start_date = start_date - timedelta(days=10)
    startDateKey = start_date.strftime('%Y%m%d')
    endDateKey = end_date.strftime('%Y%m%d')
    cursor = conn.cursor()
    sql = '''
SELECT
    url_sign,
    title,
    channel_id,
    source_name
FROM
    tb_news
WHERE
    FROM_UNIXTIME(fetch_time, '%%Y%%m%%d')
    BETWEEN %s and %s
''' % (startDateKey, endDateKey)
    cursor.execute(sql)
    newsExtraDct = {}
    for newsId, title, channelId, sourceName in cursor.fetchall():
        newsExtraDct[newsId] = (title, channelId, sourceName)
    return newsExtraDct

def getFeatureLog(sc, start_date, end_date):
    newsExtraDct = getNewsExtra(start_date, end_date)
    bcNewsExtraDct = sc.broadcast(newsExtraDct)
    featureFileLst = getSpanFileLst('samplefeature', start_date, end_date)
    def _(newsId, featuresDct):
        (title, channelId, sourceName) = bcNewsExtraDct.value[newsId]
        featuresDct['TITLE'] = title
        featuresDct['CHANNEL_ID'] = channelId
        featuresDct['SOURCE'] = sourceName
        return featuresDct
    featureLogRdd = sc.textFile(','.join(featureFileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: ('did' in attrDct) and \
                                ('news_id' in attrDct) and \
                                ('session' in attrDct) and \
                                ('features' in attrDct) and \
                                (attrDct['news_id'] in bcNewsExtraDct.value)
            ).map(
                lambda attrDct: (attrDct['did'],
                                 attrDct['session'],
                                 attrDct['news_id'],
                                 json.loads(attrDct['features']),
                                 attrDct['time'])
            ).map(
                lambda (did, sessionId, newsId, featuresDct, time): \
                        ((did, newsId), (featuresDct, time))
            ).reduceByKey(
                lambda x, y: x if x[1] >= y[1] else y
            ).map(
                lambda ((did, newsId), (featuresDct, time)): \
                        ((did, newsId), _(newsId, featuresDct))
            )
    return featureLogRdd

def maxLabelLog(sc, start_date, end_date):
    actionLogRdd = getUserActionLog(sc, start_date, end_date)
    featureLogRdd = getFeatureLog(sc, start_date, end_date)
    sampleRdd = featureLogRdd.leftOuterJoin(actionLogRdd, 256).map(
                lambda (key, (featuresDct, weight)): \
                        (key, (-1 if not weight else weight, featuresDct))
            )
    # skip above, remove users' sample without any positive action
    validUserLst = sampleRdd.map(
                lambda ((did, newsId), (weight, featuresDct)): \
                        (did, weight)
            ).groupByKey(256).filter(
                lambda (did, weightLst): \
                        sum(map(lambda val: 1 if val > 0 else 0,
                            weightLst)) >= MIN_USER_ACTION
            ).map(
                lambda (did, weightLst): did
            ).collect()
    bValidUserLst = sc.broadcast(validUserLst)
    sampleRdd = sampleRdd.filter(
                lambda ((did, newsId), (weight, featuresDct)): \
                        did in bValidUserLst.value
            ).map(
                lambda (key, (weight, featuresDct)): \
                        (-1 if not weight else weight, featuresDct)
            )
    return sampleRdd

def hashFeature(featureName):
    hashHex = hashlib.sha1(featureName).hexdigest()
    hashDec = int(hashHex, 16)
    return hashDec % FEATURE_SPACE_SIZE + 1

def discreteGapFeatures(featureName, value, sepValLst):
    sortedSepValLst = sorted(sepValLst, reverse=False)
    for idx, sepVal in enumerate(sepValLst):
        if value <= sepVal:
            return featureName + FEATURE_GAP + str(idx)
    return featureName + FEATURE_GAP + 'MAX'

def discreteIntFeatures(featureName, value, factor):
    intVal = int(value * factor)
    return featureName + FEATURE_GAP + str(intVal)

def discreteBoolFeatures(featureName, value):
    if value:
        return featureName + FEATURE_GAP + '1'
    else:
        return featureName + FEATURE_GAP + '0'

def getTitleFeature(title, finalFeatureLst):
    title = title.lower().translate(None,
            string.punctuation)
    titleWordLst = title.strip().split()
    titleCntFeature = discreteGapFeatures('TITLE_COUNT',
            len(titleWordLst), [5, 10, 15])
    finalFeatureLst.append(titleCntFeature)
    for curWord in titleWordLst:
        finalFeatureLst.append('WORD%s%s' % \
                (FEATURE_GAP, curWord.lower()))
    return finalFeatureLst

def setMetaFeature(featuresDct, finalFeatureLst):
    pictureFeature = discreteGapFeatures('PICTURE_COUNT',
            featuresDct.get('PICTURE_COUNT'), [0, 1, 3])
    finalFeatureLst.append(pictureFeature)
    videoFeature = discreteBoolFeatures('VIDEO_COUNT',
            featuresDct.get('VIDEO_COUNT'))
    finalFeatureLst.append(videoFeature)
    if 'SOURCE' in featuresDct:
        source = 'SOURCE' + FEATURE_GAP + \
                featuresDct['SOURCE'].replace(' ', '-')
        sourceFeature = discreteBoolFeatures(source, 1)
        finalFeatureLst.append(sourceFeature)
    if 'CHANNEL_ID' in featuresDct:
        channel = 'CHANNEL_ID' + FEATURE_GAP + \
                str(featuresDct['CHANNEL_ID'])
        channelFeature = discreteBoolFeatures(channel, 1)
        finalFeatureLst.append(channelFeature)
    getTitleFeature(featuresDct['TITLE'], finalFeatureLst)
    return finalFeatureLst

def setActionFeature(featuresDct, finalFeatureLst):
    gapFeatureParamsLst = [
            #('HISTORY_DISPLAY_COUNT', [100, 1000, 5000, 10000, 50000, 100000]),
            #('HISTORY_READ_COUNT', [100, 1000, 5000, 10000]),
            #('HISTORY_LIKE_COUNT', [10, 50, 100, 500, 1000]),
            ('HISTORY_COMMENT_COUNT', [1, 5, 10, 20, 50, 100]),]
    for featureName, sepValLst in gapFeatureParamsLst:
        value = featuresDct.get(featureName, 0)
        feature = discreteGapFeatures(
                featureName, value, sepValLst)
        finalFeatureLst.append(feature)
    #intFeatureParamsLst = [
    #        ('HISTORY_READ_DISPLAY_RATIO', 1000),
    #        ('HISTORY_LIKE_DISPLAY_RATIO', 1000),
    #        ('HISTORY_COMMENT_DISPLAY_RATIO', 1000),]
    #for featureName, factor in intFeatureParamsLst:
    #    value = min(1. ,featuresDct.get(featureName, 0))
    #    feature = discreteIntFeatures(
    #            featureName, value, factor)
    #    finalFeatureLst.append(feature)
    return finalFeatureLst

def formatSampleFeatures(sampleFeatureRdd):
    def _(featuresDct):
        finalFeatureLst = []
        # extract news meta features
        setMetaFeature(featuresDct, finalFeatureLst)
        # extract news action features
        setActionFeature(featuresDct, finalFeatureLst)
        return set(finalFeatureLst)
    sampleFeatureRdd = sampleFeatureRdd.mapValues(_)
    # get whole feature mapping
    featureNameSet = sampleFeatureRdd.map(
                lambda (weight, featureNameLst): set(featureNameLst)
            ).reduce(
                lambda x, y: x | y
            )
    # format sample features
    sampleFeatureRdd = sampleFeatureRdd.mapValues(
                lambda featureNameSet: [hashFeature(featureName) \
                        for featureName in featureNameSet]
            ).mapValues(
                lambda featureIdxLst: sorted(set(featureIdxLst))
            ).mapValues(
                lambda featureIdxLst: map(lambda val: '%s:1'%val,
                    featureIdxLst)
            )
    return (sampleFeatureRdd, featureNameSet)

def quotaSample(sampleRdd, posRatio, negRatio):
    def _(weight, featureIdxLst):
        if weight > 0:
            if random() <= posRatio:
                return [POSITIVE_TAG, ] + featureIdxLst
            else:
                return None
        else:
            if random() <= negRatio:
                return [NEGATIVE_TAG, ] + featureIdxLst
            else:
                return None
    os.popen('%s fs -rm -r %s' % (HADOOP_BIN, TRAINING_DATA_HDFS))
    sampleRdd = sampleRdd.map(
                lambda (weight, featureIdxLst): _(weight, featureIdxLst)
            ).filter(
                lambda sampleFeatureLst: sampleFeatureLst
            ).map(
                lambda sampleFeatureLst: ' '.join(sampleFeatureLst)
            ).saveAsTextFile(TRAINING_DATA_HDFS)
    baseName = os.path.basename(TRAINING_DATA_HDFS)
    os.popen('rm -rf %s' % os.path.join(TMP_DATA_PATH, baseName))
    os.popen('%s fs -get %s %s' % (HADOOP_BIN, TRAINING_DATA_HDFS,
        TMP_DATA_PATH))
    os.popen('cat %s/* > %s' % (os.path.join(TMP_DATA_PATH, baseName),
        SAMPLE_FILENAME))

if __name__ == '__main__':
    sc = SparkContext(appName='rerank/limeng@agilanews.com',
            pyFiles=[])
    parser = OptionParser()
    parser.add_option('-s', '--start', dest='start_date')
    parser.add_option('-e', '--end', dest='end_date')
    parser.add_option('-a', '--action', dest='action')
    parser.add_option('--clickRatio', dest='clickRatio', default=1.)
    parser.add_option('--displayRatio', dest='displayRatio', default=1.)
    parser.add_option('--dataDir', dest="dataDir", default='')
    (options, args) = parser.parse_args()

    start_date = datetime.strptime(options.start_date, '%Y%m%d').date()
    end_date = datetime.strptime(options.end_date, '%Y%m%d').date()
    global DATA_DIR
    global SAMPLE_FILENAME
    global FEAUTRE_NAME_FILENAME
    if not options.dataDir:
        print 'data directory missing, error!'
        exit(0)
    DATA_DIR = options.dataDir
    SAMPLE_FILENAME = os.path.join(DATA_DIR, 'sample.dat')
    FEAUTRE_NAME_FILENAME = os.path.join(DATA_DIR, 'featureName.mapping')
    if options.action == 'verbose':
        sampleFeatureRdd = maxLabelLog(sc, start_date, end_date)
        categoryRdd = sampleFeatureRdd.map(
                    lambda (weight, featuresDct): (weight, 1)
                ).reduceByKey(
                    lambda x, y: x + y, 64
                )
        categoryLst = categoryRdd.collect()
        print '=' * 60
        for idx, (category, cnt) in enumerate(categoryLst):
            print '%s. eventId:%s, cnt:%s' % (idx, category, cnt)
        print '=' * 60
    elif options.action == 'sample':
        sampleFeatureRdd = maxLabelLog(sc, start_date, end_date)
        (sampleRdd, featureNameLst) = formatSampleFeatures(sampleFeatureRdd)
        clickRatio = float(options.clickRatio)
        displayRatio = float(options.displayRatio)
        quotaSample(sampleRdd, clickRatio, displayRatio)
        with open(FEAUTRE_NAME_FILENAME, 'w') as fp:
            featureNameIdxDct = {}
            for featureName in featureNameLst:
                featureNameIdxDct[featureName] = hashFeature(featureName)
            sortedFeatureNameLst = sorted(featureNameIdxDct.items(),
                    key=lambda vals: vals[1], reverse=False)
            for featureName, featureIdx in sortedFeatureNameLst:
                print >>fp, '%s\t%s' % (featureIdx, featureName)
    elif options.action == 'feature':
        featureIdxDct = {}
        idxScoDct = {}
        MODEL_FILENAME = os.path.join(DATA_DIR, 'liblinear.model')
        FEATURE_START_IDX = 6
        with open(MODEL_FILENAME, 'r') as fp:
            idx = 0
            for line in fp:
                idx += 1
                if idx <= FEATURE_START_IDX:
                    continue
                idxScoDct[idx - FEATURE_START_IDX] = \
                        float(line.strip())
        with open(FEAUTRE_NAME_FILENAME, 'r') as fp:
            for line in fp:
                vals = line.strip().split('\t')
                if len(vals) != 2:
                    continue
                (featureIdx, featureName) = vals
                featureIdxDct[featureName] = \
                        idxScoDct[int(featureIdx)]
        sortedFeatureScoLst = sorted(featureIdxDct.items(),
                key=lambda val:val[1], reverse=True)
        SORTED_FEATURE_MAPPING = os.path.join(DATA_DIR,
                'featureName.mapping.score')
        with open(SORTED_FEATURE_MAPPING, 'w') as fp:
            for featureName, featureSco in sortedFeatureScoLst:
                print >>fp, '%s\t%.5f' % (featureName, featureSco)
