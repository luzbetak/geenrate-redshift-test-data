#!/bin/bash
#----------------------------------------------------------------------------------#
rm -f nohup.out 
# stop-all.sh; sleep 3; start-all.sh; sleep 3;
#----------------------------------------------------------------------------------#
spark-submit                                              \
    --packages org.apache.hadoop:hadoop-aws:2.7.7         \
    --master local[2]                                     \
    --driver-memory 4G                                    \
    --executor-memory 4G                                  \
    /root/40-apache-spark/wordcount.py                    \
    "s3a://luzbetak/input/alice29.txt"                    \
    "s3a://luzbetak/output-05/"
    
#----------------------------------------------------------------------------------#
