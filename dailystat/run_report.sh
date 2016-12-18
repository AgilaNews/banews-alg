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

function makeHadoopDir()
{
    curDate=$1
    echo $curDate
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -mkdir -p /banews/useraction/$year/$month/$day"
    echo $cmd
    eval $cmd
}

function moveFile2HadoopDir()
{
    curDate=$1
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -mv /banews/useraction.log-$year-$month-$day* /banews/useraction/$year/$month/$day"
    echo $cmd
    eval $cmd
}

function copyFile2LocalDir()
{
    curDate=$1
    outputDir=$2
    year=`date -d "$curDate" +%Y`
    month=`date -d "$curDate" +%m`
    day=`date -d "$curDate" +%d`
    cmd="$HADOOP fs -getmerge /banews/useraction/$year/$month/$day/* $outputDir"useraction_$year$month$day""
    echo $cmd
    eval $cmd
}

makeHadoopDir "$statday"
moveFile2HadoopDir "$statday"
copyFile2LocalDir "$statday" "/data/prod_stat/"
beginday=`date -d "-1 day" +%Y%m%d`
endday=`date -d "+1 day $beginday" +%Y%m%d`
#$PYTHON daily_stat.py $beginday $endday
sh run.sh daily_stat.py
sh run.sh install_stat.py
sh run.sh channelAnalysis.py 
sh run.sh video_stat.py 
sh run.sh install_stat_new.py
