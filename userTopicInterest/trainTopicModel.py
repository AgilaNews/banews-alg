# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import sys
import os
import re
import string
import urllib
from datetime import date, datetime, timedelta
import MySQLdb
import json
import pickle
from optparse import OptionParser
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import spacy
from spacy.parts_of_speech import (NOUN, ADJ, NAMES)

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
MAX_DF = 0.9
# if float, the parameter represents a
# proportion of documents, integer absoute counts
MIN_DF = 2
MAX_FEATURES = 100000
N_TOPIC = 80
MAX_ITER = 20
VALID_POS_LST = [NOUN, ADJ, NAMES]
en_nlp = spacy.load('en')

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

def loadData(dateObj, start_date, end_date):
    newsLst = []
    replacePunc = string.maketrans(string.punctuation,
            ' ' * len(string.punctuation))
    with open(os.path.join(DATA_DIR, '%s_tb_news.data' \
            % dateObj.strftime('%Y-%m-%d')), 'r') as fp:
        for line in fp:
            vals = line.strip().split('\t')
            if len(vals) < ATTRIBUTE_DIM:
                continue
            fetchTime = float(vals[FETCH_TIME])
            fetchTime = datetime.fromtimestamp(fetchTime).date()
            if (fetchTime < start_date) or (fetchTime >= end_date):
                continue
            textStr = vals[TITLE] + ' ' + stripTag(vals[CONTENT])
            textStr = textStr.encode('utf-8').translate(replacePunc)
            newsLst.append((vals[NEWS_ID], textStr.decode('utf-8')))
    return newsLst

def stemDoc(newsDoc):
    stemmedDocLst = []
    for token in newsDoc:
        if token.pos not in VALID_POS_LST:
            continue
        stemmedDocLst.append(token.lemma_)
    stemmedDocStr = ' '.join(stemmedDocLst)
    return stemmedDocStr

def preProcessSPACY(dateObj, start_date, end_date, withPreprocess):
    fileName = dateObj.strftime('%Y%m%d') + '_preprocess.dat'
    preNewsDocLst = []
    preNewsIdLst = []
    print '%s, preprocessing trainning data...' % \
            datetime.now().strftime('%s')
    if withPreprocess:
        print '%s, loading trainning data...' % \
                datetime.now().strftime('%s')
        newsLst = loadData(dateObj, start_date, end_date)
        print '%s, %s news loaded...' % \
                (datetime.now().strftime('%s'), len(newsLst))
        with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'w') as fp:
            (idLst, docLst) = zip(*newsLst)
            for idx, curDoc in enumerate(en_nlp.pipe(docLst,
                batch_size=50, n_threads=4)):
                stemmedDocStr = stemDoc(curDoc)
                newsId = idLst[idx]
                print >>fp, '%s,%s' % (newsId, stemmedDocStr)
                preNewsDocLst.append(stemmedDocStr)
            preNewsIdLst = idLst
    else:
        with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'r') as fp:
            for line in fp:
                vals = line.strip().split(',', 1)
                if len(vals) != 2:
                    continue
                (newsId, newsDoc) = vals
                preNewsDocLst.append(newsDoc)
                preNewsIdLst.append(newsId)
    return (preNewsIdLst, preNewsDocLst)

def trainLDA(dateObj, start_date, end_date, withPreprocess=False):
    (preNewsIdLst, preNewsDocLst) = preProcessSPACY(dateObj,
            start_date, end_date, withPreprocess)
    print '%s, space vector model building...' % \
            datetime.now().strftime('%s')
    vectorizer = CountVectorizer(max_df=MAX_DF,
                                 min_df=MIN_DF,
                                 max_features=MAX_FEATURES,
                                 stop_words='english')
    tfMatrix = vectorizer.fit_transform(preNewsDocLst)
    ldaModel = LatentDirichletAllocation(
                    n_topics=N_TOPIC,
                    max_iter=MAX_ITER,
                    learning_method='batch',
                    n_jobs=1,
                    verbose=1,
                    evaluate_every=5)
    print '%s, training lda model...' % \
            datetime.now().strftime('%s')
    newsTopicArr = ldaModel.fit_transform(tfMatrix)
    print '%s, dumping model...' % \
            datetime.now().strftime('%s')
    dump(vectorizer, ldaModel, newsTopicArr, preNewsIdLst)
    return (vectorizer, ldaModel)

def dump(vectorizer, ldaModel, newsTopicArr, preNewsIdLst,
        topWords=100):
    with open(os.path.join(MODEL_DIR, 'vectorizer.m'),
            'wb') as fp:
        pickle.dump(vectorizer, fp)
    with open(os.path.join(MODEL_DIR, 'ldaModel.m'),
            'wb') as fp:
        pickle.dump(ldaModel, fp)
    with open(os.path.join(MODEL_DIR, 'newsTopic.d'),
            'w') as fp:
        for idx, topicArr in enumerate(newsTopicArr):
            newsId = preNewsIdLst[idx]
            print >>fp, '%s\t%s' % (newsId,
                    ','.join(map(str, topicArr)))
    with open(os.path.join(MODEL_DIR, 'topicWord.d'),
            'w') as fp:
        featureNameLst = vectorizer.get_feature_names()
        for topicIdx, topicDis in enumerate(ldaModel.components_):
            print 'Topic:', topicIdx
            topWordStr = " ".join([featureNameLst[idx] for idx \
                    in topicDis.argsort()[:-topWords-1:-1]])
            print '\t', topWordStr
            print >>fp, '%s,%s' % (topicIdx, topWordStr)

def getSpanNews(start_date=None, end_date=None):
    conn = MySQLdb.connect(host='10.8.22.123',
                           user='banews_w',
                           passwd=urllib.quote('MhxzKhl-Happy'),
                           port=3306,
                           db='banews')
    conn.autocommit(True)
    cursor = conn.cursor()
    sqlCmd = '''
select
    url_sign,
    title,
    json_text
from
    tb_news
where
    (
        channel_id not in (10011, 10012)
    )
    and (is_visible = 1)
    and (
        date(
            from_unixtime(fetch_time)
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
    for idx, (newsId, titleStr, docStr) in enumerate(cursor.fetchall()):
        if idx % 100 == 0:
            print 'fetch %s news...' % idx
        textStr = titleStr + ' ' + stripTag(docStr)
        newsDocLst.append((newsId, textStr.decode('utf-8')))
    return newsDocLst

def loadPredictTopics():
    newsIdSet = set()
    with open(os.path.join(MODEL_DIR, 'newsTopic.d'), 'r') as fp:
        for line in fp:
            vals = line.strip().split('\t', 1)
            if len(vals) != 2:
                newsIdSet.add(vals[0])
    return newsIdSet

def predict(newsDocLst):
    with open(os.path.join(MODEL_DIR, 'ldaModel.m'), 'rb') as fp:
        ldaModel = pickle.load(fp)
    with open(os.path.join(MODEL_DIR, 'vectorizer.m'), 'rb') as fp:
        vectorizer = pickle.load(fp)
    stemmedDocLst = [], []
    (idLst, docLst) = zip(*newsDocLst)
    for idx, curDoc in enumerate(en_nlp.pipe(docLst,
            batch_size=50, n_threads=4)):
        stemmedDocStr = stemDoc(curDoc)
        stemmedDocLst.append(stemmedDocStr)
    tfMatrix = vectorizer.transform(stemmedDocLst)
    newsTopicArr = ldaModel.transform(tfMatrix)
    alreadyNewsIdSet = loadPredictTopics()
    with open(os.path.join(MODEL_DIR, 'newsTopic.d'), 'a+') as fp:
        for idx, topicArr in enumerate(newsTopicArr):
            newsId = idLst[idx]
            if newsId in alreadyNewsIdSet:
                continue
            print >>fp, '%s\t%s' % (newsId,
                    ','.join(map(str, topicArr)))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-a', '--action', dest='action', default='train')
    parser.add_option('-d', '--date', dest='date', default='20161019')
    parser.add_option('-p', '--preprocess', dest='preprocess', default=True)
    (options, args) = parser.parse_args()
    if options.action == 'train':
        dateObj = datetime.strptime(options.date, '%Y%m%d').date()
        end_date = date.today() + timedelta(days=1)
        start_date = end_date - timedelta(days=120)
        trainLDA(dateObj, start_date, end_date,
                withPreprocess=options.preprocess)
    elif options.action == 'predict':
        end_date = date.today() + timedelta(days=1)
        start_date = date(2016, 10, 18)
        newsDocLst = getSpanNews(start_date=start_date,
                                 end_date=end_date)
        print '%s new between %s and %s' % (len(newsDocLst),
                                            start_date.strftime('%Y-%m-%d'),
                                            end_date.strftime('%Y-%m-%d'))
        predict(newsDocLst)
