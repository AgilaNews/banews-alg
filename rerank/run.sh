#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7
SCALE_EXE='/home/work/limeng/banews-alg/rerank/liblinear-multicore-2.11-1/svm-scale'
LIBLINEAR_EXE='/home/work/limeng/banews-alg/rerank/liblinear-multicore-2.11-1/train'
DATA_DIR='/data/models/liblinear'
SAMPLE_FILE=$DATA_DIR/'sample.dat'
RANGE_FILE=$DATA_DIR/'sample.dat.range'
SCALE_FILE=$DATA_DIR/'sample.dat.scale'
MODEL_FILE=$DATA_DIR/'liblinear.model'
today=`date +%Y%m%d`
end_date=`date -d '1 days' +%Y%m%d`
start_date=`date -d '-14 days' +%Y%m%d`

if [ $1 = "sample" ]; then
    # positive sample ratio: 6000000 / 32717667. = 0.183
    # negative sample ratio: 1500000 / 1918506. = 0.782
    echo "sampling trainning data..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        dataGen.py -s $start_date -e $end_date -a sample \
        --clickRatio 0.79 --displayRatio 0.23 --dataDir $DATA_DIR
    echo "scaling trainning data..."
    $SCALE_EXE -s $RANGE_FILE -l 0 "$SAMPLE_FILE" > $SCALE_FILE 
fi

ACTION_ARR=("verbose", "daily")
if echo "${ACTION_ARR[@]}" | grep -w $1 &>/dev/null; then
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        dataGen.py -s $start_date -e $end_date \
        --dataDir $DATA_DIR -a $1
fi

svm_params="-s 0 -B 1 -n 5"
if [ $1 = "crossValidation" ]; then
    $LIBLINEAR_EXE $svm_params -v 5 -C $SCALE_FILE 
fi

if [ $1 = "train" ]; then
    cost=1.0
    $LIBLINEAR_EXE $svm_params -c $cost $SCALE_FILE $MODEL_FILE
fi
