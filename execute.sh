#!/bin/sh

rm -rf Output/
rm -rf temp1/
rm -rf temp2/
hdfs dfs -rm -R "$2"/Output/
hdfs dfs -rm -R "$2"/temp1/
hdfs dfs -rm -R "$2"/temp2/
hadoop jar PageRank.jar pagerank.PageRank "$1"  "$2"

hadoop fs -get "$2"/Output .
#hadoop fs -get PageRank/temp1 .
#hadoop fs -get PageRank/temp2 .
