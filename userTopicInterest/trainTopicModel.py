# -*- coding:utf-8 -*-
from __future__ import unicode_literals
import sys
import os
import re
import string
from datetime import date, datetime, timedelta
import MySQLdb
import json
import pickle
from nltk import word_tokenize
from nltk import pos_tag
from nltk.stem.lancaster import LancasterStemmer
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
MAX_ITER = 30
VALID_POS_LST = [NOUN, ADJ, NAMES]

def stripTag(htmlStr):
    reg = re.compile(r'<[^>]+>', re.S)
    return reg.sub('', htmlStr)

def loadData(dateObj):
    newsLst = []
    replacePunc = string.maketrans(string.punctuation,
            ' ' * len(string.punctuation))
    with open(os.path.join(DATA_DIR, '%s_tb_news.data' \
            % dateObj.strftime('%Y-%m-%d')), 'r') as fp:
        for line in fp:
            vals = line.strip().split('\t')
            if len(vals) < ATTRIBUTE_DIM:
                continue
            textStr = vals[TITLE] + ' ' + stripTag(vals[CONTENT])
            textStr = textStr.encode('utf-8').translate(replacePunc)
            newsLst.append(textStr.decode('utf-8'))
    return newsLst

def preProcessNLTK(newsLst, validTags=['NN','JJ','NNP']):
    preNewsLst = []
    stemmer = LancasterStemmer()
    for curNews in newsLst:
        tokenLst = word_tokenize(curNews)
        stemmedLst = []
        for curWord in tokenLst:
            stemmedLst.append(stemmer.stem(curWord))
        wordTagLst = pos_tag(stemmedLst)
        filteredTagLst = [word for (word, tag) \
                in wordTagLst if tag in validTags]
        preNewsLst.append(' '.join(filteredTagLst))
    return preNewsLst

def preProcessSPACY(dateObj, withPreprocess):
    fileName = dateObj.strftime('%Y%m%d') + '_preprocess.dat'
    preNewsLst = []
    print 'preprocessing trainning data...'
    if withPreprocess:
        print 'loading trainning data...'
        newsLst = loadData(dateObj)
        print '%s news loaded...' % len(newsLst)
        en_nlp = spacy.load('en')
        with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'w') as fp:
            for idx, curDoc in enumerate(en_nlp.pipe(newsLst,
                batch_size=50, n_threads=4)):
                stemmedDocLst = []
                for token in curDoc:
                    if token.pos not in VALID_POS_LST:
                        continue
                    stemmedDocLst.append(token.lemma_)
                stemmedDocStr = ' '.join(stemmedDocLst)
                print >>fp, stemmedDocStr
                preNewsLst.append(stemmedDocStr)
    else:
        with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'r') as fp:
            for line in fp:
                preNewsLst.append(line)
    return preNewsLst

def trainLDA(dateObj, withPreprocess=False):
    preNewsLst = preProcessSPACY(dateObj, withPreprocess)
    print 'space vector model building...'
    vectorizer = CountVectorizer(max_df=MAX_DF,
                                 min_df=MIN_DF,
                                 max_features=MAX_FEATURES,
                                 stop_words='english')
    tfMatrix = vectorizer.fit_transform(preNewsLst)
    ldaModel = LatentDirichletAllocation(
                    n_topics=N_TOPIC,
                    max_iter=MAX_ITER,
                    learning_method='online')
    print 'training lda model...'
    ldaModel.fit(tfMatrix)
    with open(os.path.join(KDIR, 'data', 'countVector.m'),
            'wb') as fp:
        pickle.dump(vectorizer, fp)
    with open(os.path.join(KDIR, 'data', 'lda.m'), 'wb') as fp:
        pickle.dump(ldaModel, fp)
    print 'dumping model...'
    return (vectorizer, ldaModel)

def debug(ldaModel, vectorizer, topWords=30):
    featureNameLst = vectorizer.get_feature_names()
    for topicIdx, topicDis in enumerate(ldaModel.components_):
        print 'Topic:', topicIdx
        topWordStr = " ".join([featureNameLst[idx] for idx \
                in topicDis.argsort()[:-topWords-1:-1]])
        print '\t', topWordStr

if __name__ == '__main__':
    dateObj = date(2016, 9, 18)
    (vectorizer, ldaModel) = trainLDA(dateObj, withPreprocess=True)
    debug(ldaModel, vectorizer)
