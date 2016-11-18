#!/bin/bash

export PYSPARK_PYTHON=/usr/bin/python2.7
today=`date +%Y%m%d`
end_date=`date -d '1 days' +%Y%m%d`
start_date=`date -d '-14 days' +%Y%m%d`

if [ $1 = "sample" ]; then
    # positive sample ratio: 6000000 / 32717667. = 0.183
    # negative sample ratio: 1500000 / 1918506. = 0.782
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        dataGen.py -s $start_date -e $end_date -a sample \
        --clickRatio 0.79 --displayRatio 0.23
fi

if [ $1 = "verbose" ]; then
    /home/work/spark-1.6.2-bin-ba/bin/spark-submit \
        --master yarn-client --executor-memory 2G \
        --num-executors 10 --executor-cores 4 \
        --driver-memory 4G --conf spark.akka.frameSize=100 \
        --conf spark.shuffle.manager=SORT \
        --conf spark.yarn.executor.memoryOverhead=4096 \
        --conf spark.driver.maxResultSize=4096 \
        --conf spark.yarn.driver.memoryOverhead=4096 \
        dataGen.py -s $start_date -e $end_date -a verbose
fi
