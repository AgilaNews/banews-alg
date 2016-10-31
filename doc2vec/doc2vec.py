# -*- coding:utf-8 -*-
import sys
import os
import pickle
import numpy
import gensim
import random

from datetime import date, datetime
from optparse import OptionParser
from gensim.models.doc2vec import Doc2Vec, LabeledSentence

DATA_DIR = '/data/mysqlBackup/banews'
PREPROCESS_DATA_DIR = '/data/userTopicDis'
MODEL_DIR = '/data/doc2vec/model'
SIM_FILE = '/data/doc2vec/similarity.txt'

def loadData(dateObj):
    fileName = dateObj.strftime('%Y%m%d') + '_preprocess.dat'
    labelized = []
    print '%s, loading preprocess data...' % \
            datetime.now().ctime()
    with open(os.path.join(PREPROCESS_DATA_DIR, fileName), 'r') as fp:
        for line in fp:
            vals = line.strip().split(',', 1)
            if len(vals)!=2:
                continue
            (newsId, newsDoc) = vals
            labelized.append(LabeledSentence(
                                words=newsDoc.split(),
                                tags=[newsId]))
    return labelized[:10000]

def trainDoc2Vec(labelized, size=400, epoch_num=10, model=None):
    if not model:
        model = Doc2Vec(min_count=1, window=10, size=size,
                            sample=1e-5, negative=5, workers=2)

    print '%s, building volcabulary for models...' % \
            datetime.now().ctime()
    model.build_vocab(labelized)

    for epoch in range(epoch_num):
        print '%s, Epoch %s, training models...' %(datetime.now().ctime(), epoch)
        random.shuffle(labelized)
        model.train(labelized)
    return model

def dump(model):
    with open(os.path.join(MODEL_DIR, 'model.m'), 'wb') as fp:
        pickle.dump(model, fp)

def loadModel():
    with open(os.path.join(MODEL_DIR, 'model.m'), 'rb') as fp:
        model = pickle.load(fp)
    return model

def saveSimilarity(dataset, model):
    with open(SIM_FILE, 'w') as fp:
        for item in docs:
            wordlist = item[0]
            idx = item[1][0]
            vector = model.infer_vector(wordlist)
            sims = model.docvecs.most_similar([vector],topn=8)
            print "***Similar Docs for Article %s ***" % idx
            print sims
            fp.write(idx)
            fp.write('\t')
            fp.write(str(sims))
            fp.write('\n')


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-a', '--action', dest='action', default='train')
    parser.add_option('-d', '--date', dest='date', default='20161019')
    (options, args) = parser.parse_args()

    if options.action == 'train':
        #train basic model and save it
        startTime = datetime.now()
        dateObj = datetime.strptime(options.date, '%Y%m%d').date()
        docs = loadData(dateObj)
        model = trainDoc2Vec(docs)
        dump(model)
        saveSimilarity(docs, model)
        print datetime.now() - startTime

    elif options.action == 'update':
        #re-train the original date with updated data
        startTime = datetime.now()
        model = loadModel()

        dateObj = datetime.strptime(options.date, '%Y%m%d').date()
        new_docs = loadData(dateObj)
        trainDoc2Vec(new_docs, model=model)


