#!/bin/bash
/home/work/spark-1.6.2-bin-ba/bin/spark-submit \
    --master yarn-client --executor-memory 1G \
    --num-executors 4 --executor-cores 4 \
    --driver-memory 1G --conf spark.akka.frameSize=100 \
    --conf spark.shuffle.manager=SORT \
    --conf spark.yarn.executor.memoryOverhead=4096 \
    --conf spark.yarn.driver.memoryOverhead=4096 $1
