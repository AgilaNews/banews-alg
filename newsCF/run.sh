#!/bin/bash

CUR_DIR=`dirname $(pwd)/${0}`
HADOOP_BIN=/home/work/hadoop-2.6.0-cdh5.7.0/bin/hadoop
JAR_NAME=${CUR_DIR}/calcSim.jar

sim_input_dir='/data/calcSim/input'
sim_output_dir='/data/calcSim/output'
USER_MIN_THRE=0

$HADOOP_BIN fs -rm -r -skipTrash $sim_output_dir
$HADOOP_BIN jar $JAR_NAME -i $sim_input_dir -o $sim_output_dir \
    --inputFormat fv --ibarMax 600 --ibarMethod sample \
    --ubarMax 400 --ubarMin $USER_MIN_THRE \
    --ubarMethod normal --mapperNum 8 --reducerNum 8 \
    --method cosine --taskQueue root.hadoop-recsys.recomm 

