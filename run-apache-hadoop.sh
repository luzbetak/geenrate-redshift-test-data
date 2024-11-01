#!/bin/bash
#----------------------------------------------------------------------------------#
#  --executor-memory 16G                                 \
#  --total-executor-cores 50                             \
#  --num-executors   4                                   \
#  --executor-cores  8                                   \
#  --driver-memory   16G                                 \
#  --executor-memory 4G                                  \
#----------------------------------------------------------------------------------#
# rm -f nohup.out 
# stop-all.sh; sleep 5; start-all.sh; sleep 5;
#----------------------------------------------------------------------------------#
#      --master spark://172.31.9.145:7077               \
#----------------------------------------------------------------------------------#
for i in {123..123}
do
    n=`printf %03d $i`
    echo "Slice: $n"
    nohup spark-submit                                  \
      --packages org.apache.hadoop:hadoop-aws:2.7.7     \
      --master local[2]                                 \
      --driver-memory 2G                                \
      --executor-memory 1G                              \
      /root/40-apache-spark/21-generate-data.py         \
      "$i"                                              \
      "100000001"                                       \
      "s3a://luzbetak/table_001/slice-$n" &
    
    sleep 10

done
#----------------------------------------------------------------------------------#
#   "s3a://luzbetak/slice-001" &
#   # "s3a://luzbetak/parquet-24"
#----------------------------------------------------------------------------------#
# spark-submit                                         \
#   --driver-memory   16G                              \
#   --num-executors   4                                \
#   --executor-cores  4                                \
#   --executor-memory 4G                               \
#   --master spark://172.31.9.145:7077                 \
#   21-generate-data.py
#----------------------------------------------------------------------------------#
