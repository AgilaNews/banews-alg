#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7
SCALE_EXE='/home/work/limeng/banews-alg/rerank/liblinear-multicore-2.11-1/svm-scale'
TRAIN_EXE='/home/work/limeng/banews-alg/rerank/liblinear-multicore-2.11-1/train'
DATA_DIR='/data/models/liblinear'
SAMPLE_FILE=$DATA_DIR/'sample.dat'
RANGE_FILE=$DATA_DIR/'sample.dat.range'
SCALE_FILE=$DATA_DIR/'sample.dat.scale'
MODEL_FILE=$DATA_DIR/'liblinear.model'
today=`date +%Y%m%d`
end_date=`date -d '1 days' +%Y%m%d`
start_date=`date -d '-28 days' +%Y%m%d`
debug=false
PYTHON=/usr/bin/python2.7

SCRIPT_NAME='discreteDataGen.py'
if [ $1 = "sample" ]; then
    #topic_start_date=`date -d '-15 days' +%Y%m%d`
    #$PYTHON /home/work/banews-alg/userTopicInterest/trainTopicModel.py \
    #    -a predict_offline -e $end_date -s $topic_start_date 
    # negative sample ratio: 6000000 / 32717667. = 0.183
    # positive sample ratio: 1500000 / 1918506. = 0.782
    echo "sampling trainning data..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 3 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=6g \
        ${SCRIPT_NAME} -s $start_date -e $end_date -a sample \
        --clickRatio 0.79 --displayRatio 0.23 --dataDir $DATA_DIR
    echo "scaling trainning data..."
    #$SCALE_EXE -s $RANGE_FILE -l 0 "$SAMPLE_FILE" > $SCALE_FILE 
fi

if [ $1 = "feature" ]; then
    echo "feature model data..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 3 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=6g \
        ${SCRIPT_NAME} -s $start_date -e $end_date -a feature \
        --dataDir $DATA_DIR
fi

ACTION_ARR=("verbose", "daily")
if echo "${ACTION_ARR[@]}" | grep -w $1 &>/dev/null; then
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 3 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        ${SCRIPT_NAME} -s $start_date -e $end_date \
        --dataDir $DATA_DIR -a $1
fi

svm_params="-s 0 -B 1 -n 5"
if [ $1 = "search" ]; then
    $TRAIN_EXE $svm_params -v 5 -C $SAMPLE_FILE
fi

#cost=0.015625
#cost=0.00195312
#cost=0.00390625
cost=0.125
if [ $1 = "crossValidation" ]; then
    $TRAIN_EXE $svm_params -v 5 -c $cost $SAMPLE_FILE 
fi

if [ $1 = "train" ]; then
    $TRAIN_EXE $svm_params -c $cost $SAMPLE_FILE $MODEL_FILE
  if [ "$debug" = true ]; then
      sandbox="10.8.6.7"
      echo 'scp to sandbox@'${sandbox}
      scp -r /data/models/liblinear root@$sandbox:/data/models/
      ssh root@$sandbox "chown -R work:work /data/models/liblinear"
  else
      comment="10.8.91.237"
      echo 'scp to comment@'${comment}
      scp -r /data/models/liblinear root@$comment:/data/models/
      ssh root@$comment "chown -R work:work /data/models/liblinear"
  fi
fi
