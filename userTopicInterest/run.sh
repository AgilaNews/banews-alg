#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7

PYTHON_BIN='/usr/bin/python2.7'
today=`date +%Y%m%d`

# backup news data from mysql
if [ $1 = "mysql_backup" ]; then
    echo "mysql backup util ", $today
    $PYTHON_BIN mysqlBackup.py
fi

# train topic model based on news
if [ $1 = "model_train" ]; then
    echo "model training..."
    $PYTHON_BIN trainTopicModel.py -d $today -a train > train.log 2>&1
fi

# calculate users' topic interest distribution
if [ $1 = "user_interest" ]; then
    echo "appending news topic distribution..."
    end_date=`date -d '1 days' +%Y%m%d`
    start_date=`date -d '-10 days' +%Y%m%d`
    $PYTHON_BIN trainTopicModel.py -a predict_offline -s $start_date \
        -e $end_date > user_interest.log 2>&1
    echo "user interest calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        calUserInterest.py >> user_interest.log 2>&1
fi

# calcualte recent topic click distribution, 
# and recent news score in one days
if [ $1 = "recent_score" ]; then
    echo "topic distribution & news score calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 2G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=2048 \
        --conf spark.yarn.driver.memoryOverhead=2048 \
        recentNewsInfo.py > recent_score.log 2>&1
fi

