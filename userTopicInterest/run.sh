#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7

PYTHON_BIN='/usr/bin/python2.7'
today=`date +%Y%m%d`

# backup news data from mysql
generateData() {
    echo "mysql backup util ", $today
    $PYTHON_BIN mysqlBackup.py
    $PYTHON_BIN trainTopicModel.py -d $today -a preprocess
}

trainModel() {
    echo "model training..."
    $PYTHON_BIN trainTopicModel.py -d $today -a train 
}

calcUserTopic() {
    # calculate users' topic interest distribution
    echo "appending news topic distribution..."
    end_date=`date -d '1 days' +%Y%m%d`
    start_date=`date -d '-10 days' +%Y%m%d`
    $PYTHON_BIN trainTopicModel.py -a predict_offline -s $start_date \
        -e $end_date > user_interest.log 2>&1
    echo "user interest calculation..."
    end_date=`date -d '0 days' +%Y%m%d`
    start_date=`date -d '-30 days' +%Y%m%d`
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 3 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        calUserInterest.py -a user_interest -s $start_date \
        -e $end_date >> user_interest.log 2>&1
}

calcVideoInterest(){
# calculate users' video interest distribution
    echo "video channel calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 2 --executor-cores 2 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        user_videochannel.py

    echo "video interest calculation..."
    end_date=`date -d '0 days' +%Y%m%d`
    start_date=`date -d '-30 days' +%Y%m%d`
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 2 --executor-cores 2 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        userVideoInterest.py -a video_interest -s $start_date \
        -e $end_date >> video_interest.log 2>&1
}


calcNewsTopic() {
    # calculate recent topic click distribution, 
    # and recent news score in one days
    echo "topic distribution & news score calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 2G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=2048 \
        --conf spark.yarn.driver.memoryOverhead=2048 \
        recentNewsInfo.py > recent_score.log 2>&1
}

# calcualte recent topic click distribution, 
# and recent news score in one days

case $1 in 
    data)
        generateData
        exit 0
        ;;
    train)
        trainModel
        exit 0
        ;;
    calc_user_topic)
        calcUserTopic
        exit 0
        ;;
    calc_news_topic)
        calcNewsTopic
        exit 0
        ;;
    calc_video_interest)
        calcVideoInterest
        exit 0
        ;;
    *)
        echo "./run.sh dump|calc_user_topic|calc_news_topic|calc_video_interest"
        exit -1;
esac
