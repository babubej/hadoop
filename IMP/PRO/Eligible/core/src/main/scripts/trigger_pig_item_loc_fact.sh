#! /bin/bash

#hdfs dfs -get "$8" ./

pig -Dpig.maxCombinedSplitSize=268435456 -Dmapred.max.split.size=268435456 -Dmapred.job.queue.name=etl -x tez -param $1 -param $2 -param $3 -param $4 -param $5 -param $6 -param $7 -f ./$9
