#!/bin/bash

if [[ -z $HADOOP_HOME ]]; then
    HADOOP_HOME=/opt/hadoop-1.1.2
fi

job_name=wordcount

output_local=$1

input_hdfs=/usr/${USER}/${job_name}/input
if [[ $# > 1 ]]; then
    input_hdfs=$2
fi

output_hdfs=/usr/${USER}/${job_name}/output
if [[ $# > 2 ]]; then
    output_hdfs=$3
fi

hadoop_exec="$HADOOP_HOME/bin/hadoop"

mkdir $output_local
${hadoop_exec} dfs -copyToLocal -crc $output_hdfs/* $output_local
${hadoop_exec} dfs -rmr -skipTrash $output_hdfs
${hadoop_exec} dfs -rmr -skipTrash $input_hdfs