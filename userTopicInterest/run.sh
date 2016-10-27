#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7

PYTHON_BIN='/usr/bin/python2.7'
mysql_backup=false
model_train=false
user_interest=true
recent_score=false
today=`date +%Y%m%d`

# backup news data from mysql
if [ "$mysql_backup" = true ]; then
    echo "mysql backup util ", $today
    $PYTHON_BIN mysqlBackup.py
fi

# train topic model based on news
if [ "$model_train" = true ]; then
    echo "model training..."
    $PYTHON_BIN trainTopicModel.py -d $today -a train > train.log 2>&1
fi

# calculate users' topic interest distribution
if [ "$user_interest" = true ]; then
    echo "user interest calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        calUserInterest.py
fi

# calcualte recent topic click distribution, 
# and recent news score in one days
if [ "$recent_score" = true ]; then
    echo "topic distribution & news score calculation..."
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 1G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 2G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=2048 \
        --conf spark.yarn.driver.memoryOverhead=2048 \
        recentNewsInfo.py
fi


