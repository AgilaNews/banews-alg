#!/bin/bash
CURDIR=`dirname $0`
HADOOP_HOME=/home/work/hadoop-2.6.0-cdh5.7.0/
HADOOP=$HADOOP_HOME/bin/hadoop
PYTHON=/usr/bin/python

#if [ $# -ne 2 ];then
#    echo parameter error, please input start date and end date
#    echo "e.g. sh run_report.sh 20160516 20160517"
#fi

statday=`date -d "-1 day"`

if [ $1 == 'useraction' ]; then
    destinationDirPre='/banews/useraction'
    originalDirPre='/banews/useraction.log'   
    localDirPre='/data/prod_stat/useraction'
elif [ $1 == 'samplefeature' ]; then
    destinationDirPre='/banews/samplefeature'
    originalDirPre='/banews/samplefeature/samplefeature.log'
else
    echo "parameter should be 'useraction' or 'samplefeature'!"
    exit 1
fi

function makeHadoopDir()
{
    curDate=$1
    echo $curDate
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -mkdir -p \
        ${destinationDirPre}/$year/$month/$day"
    echo $cmd
    eval $cmd
}

function moveFile2HadoopDir()
{
    curDate=$1
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -mv \
        ${originalDirPre}-$year-$month-$day* \
        ${destinationDirPre}/$year/$month/$day"
    echo $cmd
    eval $cmd
}

function copyFile2LocalDir()
{
    curDate=$1
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -getmerge \
        ${destinationDirPre}/$year/$month/$day/* \
        ${localDirPre}_${year}${month}${day}"
    echo $cmd
    eval $cmd
}

makeHadoopDir "$statday"
moveFile2HadoopDir "$statday"
if [ $1 == 'useraction' ]; then
    copyFile2LocalDir "$statday"
    sh run.sh daily_stat.py
    sh run.sh install_stat.py
    sh run.sh channelAnalysis.py 
    sh run.sh video_stat.py 
    sh run.sh install_stat_new.py
fi
