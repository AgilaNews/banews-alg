#!/bin/bash
export PYSPARK_PYTHON=/usr/bin/python2.7

/home/work/spark-1.6.2-bin-ba/bin/spark-submit \
    --master yarn-client --executor-memory 2G \
    --num-executors 10 --executor-cores 4 \
    --driver-memory 2G --conf spark.akka.frameSize=100 \
    --conf spark.shuffle.manager=SORT \
    --conf spark.yarn.executor.memoryOverhead=4096 \
    --conf spark.yarn.driver.memoryOverhead=2048 $1
