# -*- coding:utf-8 -*-

import sys
import os
import re
from datetime import date, datetime, timedelta
from random import random
import json
from optparse import OptionParser

from pyspark import SparkContext

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
ROUND_CNT = 5
POSITIVE_TAG = '1'
NEGATIVE_TAG = '-1'
FEATURE_MAP_NAME = 'feature_mapping.listPage'
MIN_FEATURE_VALUE = 0.001
TOPIC_CNT = 80
TOPIC_PREFIX_KEY = 'TOPIC_%s'
TRAINING_DATA_HDFS = '/user/limeng/models/liblinear/trainingData'
TMP_DATA_PATH = '/data/tmp/alg'

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

def getFeatureLog(sc, start_date, end_date):
    featureFileLst = getSpanFileLst('samplefeature', start_date, end_date)
    featureLogRdd = sc.textFile(','.join(featureFileLst)).map(
                lambda attrStr: json.loads(attrStr)
            ).filter(
                lambda attrDct: ('did' in attrDct) and \
                                ('news_id' in attrDct) and \
                                ('session' in attrDct) and \
                                ('features' in attrDct)
            ).map(
                lambda attrDct: (attrDct['did'],
                                 attrDct['session'],
                                 attrDct['news_id'],
                                 attrDct['features'],
                                 attrDct['time'])
            ).map(
                lambda (did, sessionId, newsId, featuresDct, time): \
                        ((did, newsId), (featuresDct, time))
            ).reduceByKey(
                lambda x, y: x if x[1] >= y[1] else y
            ).mapValues(
                lambda (featuresDct, time): featuresDct
            )
    return featureLogRdd

def maxLabelLog(sc, start_date, end_date):
    actionLogRdd = getUserActionLog(sc, start_date, end_date)
    featureLogRdd = getFeatureLog(sc, start_date, end_date)
    sampleRdd = featureLogRdd.leftOuterJoin(actionLogRdd, 128).map(
                lambda (key, (featuresDct, weight)): \
                        (-1 if not weight else weight, featuresDct)
            )
    return sampleRdd

def getNewsTopicFeatures(newsFeatureDct):
    filterNewsFeatureDct = {}
    with open('/data/models/lda/newsTopic.d', 'r') as fp:
        idx = 0
        for line in fp:
            if idx % 100 == 0:
                print 'load %s news topic features...'  % idx
            idx += 1
            vals = line.strip().split()
            if len(vals) != 2:
                continue
            newsId = vals[0]
            if newsId not in newsFeatureDct:
                continue
            featureLst = list(newsFeatureDct[newsId])
            topicLst = \
                    [float(curVal) for curVal in vals[1].split(',')]
            featureLst.append(topicLst)
            filterNewsFeatureDct[newsId] = featureLst
    return filterNewsFeatureDct

def loadFeatureMap(featureFile):
    if not hasattr(loadFeatureMap, 'featureNameLst'):
        featureNameLst = []
        if not os.path.isfile(featureFile):
            print 'AG feature file not exist!'
            exit(1)
        with open(featureFile, 'r') as fp:
            for line in fp:
                featureNameLst.append(line.strip())
        loadFeatureMap.featureNameLst = featureNameLst
    return loadFeatureMap.featureNameLst

def setMetaFeature(featuresDct, formatedFeaturesDct):
    formatedFeaturesDct['PICTURE_COUNT'] = \
            featuresDct.get('PICTURE_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['VIDEO_COUNT'] = \
            featuresDct.get('VIDEO_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['TITLE_LENGTH'] = \
            featuresDct.get('TITLE_LENGTH', MIN_FEATURE_VALUE)
    formatedFeaturesDct['CONTENT_LENGTH'] = \
            featuresDct.get('CONTENT_LENGTH', MIN_FEATURE_VALUE)
    formatedFeaturesDct['FETCH_TIMESTAMP_INTERVAL'] = \
            featuresDct.get('FETCH_TIMESTAMP_INTERVAL', MIN_FEATURE_VALUE)
    formatedFeaturesDct['POST_TIMESTAMP_INTERTVAL'] = \
            featuresDct.get('POST_TIMESTAMP_INTERTVAL', MIN_FEATURE_VALUE)
    return formatedFeaturesDct

def setActionFeature(featuresDct, formatedFeaturesDct):
    formatedFeaturesDct['HISTORY_DISPLAY_COUNT'] = \
            featuresDct.get('HISTORY_DISPLAY_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['HISTORY_READ_COUNT'] = \
            featuresDct.get('HISTORY_READ_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['HISTORY_LIKE_COUNT'] = \
            featuresDct.get('HISTORY_LIKE_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['HISTORY_COMMENT_COUNT'] = \
            featuresDct.get('HISTORY_COMMENT_COUNT', MIN_FEATURE_VALUE)
    formatedFeaturesDct['HISTORY_READ_DISPLAY_RATIO'] = \
            min(1., float(featuresDct.get('HISTORY_READ_DISPLAY_RATIO',
            MIN_FEATURE_VALUE)))
    formatedFeaturesDct['HISTORY_LIKE_DISPLAY_RATIO'] = \
            min(1., float(featuresDct.get('HISTORY_LIKE_DISPLAY_RATIO',
            MIN_FEATURE_VALUE)))
    formatedFeaturesDct['HISTORY_COMMENT_DISPLAY_RATIO'] = \
            min(1., float(featuresDct.get('HISTORY_COMMENT_DISPLAY_RATIO',
            MIN_FEATURE_VALUE)))
    return formatedFeaturesDct

def formatSampleFeatures(sampleFeatureRdd):
    featureMapPath = os.path.join(KDIR, FEATURE_MAP_NAME)
    featureNameLst = loadFeatureMap(featureMapPath)
    def _(featuresDct):
        formatedFeaturesDct = {}
        # extract news meta features
        setMetaFeature(featuresDct, formatedFeaturesDct)
        # extract news action features
        setActionFeature(featuresDct, formatedFeaturesDct)
        # label samples
        valueLst = []
        featureIdxLst = []
        for idx, curFeatureName in enumerate(featureNameLst):
            featureVal = formatedFeaturesDct.get(curFeatureName, 0)
            if type(featureVal) != float:
                featureVal = float(featureVal)
            featureVal = round(featureVal, ROUND_CNT)
            if featureVal:
                featureIdxLst.append('%s:%s' % (idx+1, featureVal))
        return featureIdxLst
    sampleFeatureRdd = sampleFeatureRdd.mapValues(
                lambda featuresDctStr: json.loads(featuresDctStr)
            ).mapValues(_)
    return sampleFeatureRdd

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
    sampleFeatureRdd = maxLabelLog(sc, start_date, end_date)
    global DATA_DIR
    global SAMPLE_FILENAME
    if not options.dataDir:
        print 'data directory missing, error!'
        exit(0)
    DATA_DIR = options.dataDir
    SAMPLE_FILENAME = os.path.join(DATA_DIR, 'sample.dat')
    if options.action == 'verbose':
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
        sampleRdd = formatSampleFeatures(sampleFeatureRdd)
        clickRatio = float(options.clickRatio)
        displayRatio = float(options.displayRatio)
        quotaSample(sampleRdd, clickRatio, displayRatio)
