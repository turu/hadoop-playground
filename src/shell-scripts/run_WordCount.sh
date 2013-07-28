#!/bin/bash

if [[ -z $HADOOP_HOME ]]; then
    HADOOP_HOME=/opt/hadoop-1.1.2
fi

job_name=wordcount
job_executable=lib/hadoop_playground-1.0-SNAPSHOT.jar
job_main_class=pl.turu.hadoop.mapreduce.wordcount.WordCount

input_local=$1

output_local=""
if [[ $# > 1 ]]; then
    output_local=$2
fi

input_hdfs=/usr/${USER}/${job_name}
if [[ $# > 2 ]]; then
    input_hdfs=$3
fi

output_hdfs=/usr/${USER}/${job_name}

if [[ $# > 3 ]]; then
    output_hdfs=$4
fi

pushd $HADOOP_HOME
bin/hadoop dfs -mkdir $input_hdfs
bin/hadoop dfs -copyFromLocal $input_local/* $input_hdfs/
bin/hadoop dfs -mkdir $output_hdfs

bin/hadoop jar $job_executable $job_main_class $input_hdfs $output_hdfs

mkdir $output_local
bin/hadoop dfs -copyToLocal -ignoreCrc $output_hdfs/* $output_local/
popd

