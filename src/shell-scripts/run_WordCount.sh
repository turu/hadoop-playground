#!/bin/bash

if [[ -z $HADOOP_HOME ]]; then
    HADOOP_HOME=/opt/hadoop-1.1.2
fi

job_name=wordcount
job_executable=lib/hadoop_playground-1.0-SNAPSHOT.jar

input_local=$1

input_hdfs=/usr/${USER}/${job_name}/input
if [[ $# > 1 ]]; then
    input_hdfs=$2
fi

output_hdfs=/usr/${USER}/${job_name}/output

if [[ $# > 2 ]]; then
    output_hdfs=$3
fi

hadoop_exec="$HADOOP_HOME/bin/hadoop"
${hadoop_exec} dfs -mkdir $input_hdfs
${hadoop_exec} dfs -copyFromLocal $input_local/* $input_hdfs/

${hadoop_exec} jar $job_executable $input_hdfs $output_hdfs

