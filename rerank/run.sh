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
start_date=`date -d '-2 days' +%Y%m%d`
PYTHON=/usr/bin/python2.7
SCRIPT_NAME='discreteDataGen.py'

extractSample() {
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
        --clickRatio 0.90 --displayRatio 0.18 --dataDir $DATA_DIR
    echo "scaling trainning data..."
    #$SCALE_EXE -s $RANGE_FILE -l 0 "$SAMPLE_FILE" > $SCALE_FILE 
}

extractFeature() {
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
}

verboseSample() {
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 3 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        ${SCRIPT_NAME} -s $start_date -e $end_date \
        --dataDir $DATA_DIR -a verbose
}

svm_params="-s 0 -B 1 -n 5"
searchParam() {
    $TRAIN_EXE $svm_params -v 5 -C $SAMPLE_FILE
}

cost=0.015625
crossValidationParam() {
    $TRAIN_EXE $svm_params -v 5 -c $cost $SAMPLE_FILE 
}

trainModel() {
    $TRAIN_EXE $svm_params -c $cost $SAMPLE_FILE $MODEL_FILE
}

deployServer() {
    if [ $2 = "online" ]; then
        comment="10.8.91.237"
        echo 'scp to comment@'${comment}
        scp -r /data/models/liblinear root@$comment:/data/models/
        ssh root@$comment "chown -R work:work /data/models/liblinear"
    else
        sandbox="10.8.6.7"
        echo 'scp to sandbox@'${sandbox}
        scp -r /data/models/liblinear root@$sandbox:/data/models/
        ssh root@$sandbox "chown -R work:work /data/models/liblinear"
    fi
}

case $1 in
    sample)
        extractSample
        exit 0
        ;;
    verbose)
        verboseSample
        exit 0
        ;;
    search)
        searchParam
        exit 0
        ;;
    crossValidation)
        crossValidationParam
        exit 0
        ;;
    train)
        trainModel
        exit 0
        ;;
    feature)
        extractFeature
        exit 0
        ;;
    deploy)
        extractSample
        trainModel
        extractFeature
        deployServer
        exit 0
        ;;
    *)
        echo "./run.sh sample|feature|verbose|search|crossValidation|train"
        exit -1;
esac
