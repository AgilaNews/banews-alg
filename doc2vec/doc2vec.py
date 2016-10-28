# -*- coding:utf-8 -*-
import sys
import os
import pickle
import numpy

import gensim
from gensim.models.doc2vec import Doc2Vec, LabeledSentence

KDIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = '/data/mysqlBackup/banews'
PREPROCESS_DATA_DIR = '/data/userTopicDis'
MODEL_DIR = '/data/doc2vec/model'

def loadData(dateObj):
    fileName = dateObj.strftime('%Y%m%d') + '_preprocess.dat'
    labelized = []
    print '%s, loading preprocess data...' % \
            datetime.now().strftime('%s')
    with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'r') as fp:
        for line in fp:
            vals = line.strip().split(',', 1)
            if len(vals) != 2:
                continue
            (newsId, newsDoc) = vals
            labelized.append(LabeledSentence(newsDoc, newsId))
    return numpy.array(labelized)

def trainDoc2Vec(labelized, size=400, epoch_num=10) 
    model_dm = Doc2Vec(min_count=1, window=10, size=size, sample=1e-5, negative=5, workers=2)
    model_dbow = Doc2Vec(min_count=1, window=10, size=size, sample=1e-5, negative=5, dm=0, workers=2)
    model_dm.build_vocab(labelized)
    model_dbow.build_vocab(labelized)
    for epoch in range(epoch_num):
        print 'Traing Doc2Vec Model: %s times...' % epoch
        random.shuffle(labelized)
        model_dm.train(labelized)
        model_dbow.train(labelized)
    return model_dm, model_dbow

def dump(model_dm, model_dbow):
    with open(os.path.join(MODEL_DIR, 'model_dm.m'),
            'wb') as fp:
        pickle.dump(model_dm, fp)
    with open(os.path.join(MODEL_DIR, 'model_dbow.m'),
            'wb') as fp:
        pickle.dump(mode_dbow, fp)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-a', '--action', dest='action', default='train')
    parser.add_option('-d', '--date', dest='date', default='20161019')
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
