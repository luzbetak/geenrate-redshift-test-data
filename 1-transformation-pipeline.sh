#!/bin/bash

git rev-list master | while read rev; do git ls-tree -lr $rev  | cut -c54- | sed -r 's/^ +//g;'; done  | sort -u | perl -e 'while (<>) { chomp; @stuff=split("\t");$sums{$stuff[1]} += $stuff[0];} print "$sums{$_} $_\n" for (keys %sums);' | sort -rn | head -n 20

# Distribure the code
#----------------------------------------------------------------#
scp 40-apache-spark/go root@192.168.1.10:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.11:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.12:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.13:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.14:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.15:~/40-apache-spark
scp 40-apache-spark/go root@192.168.1.16:~/40-apache-spark

scp 40-apache-spark/21* root@192.168.1.17:~/40-apache-spark
scp 40-apache-spark/21* root@192.168.1.18:~/40-apache-spark
scp 40-apache-spark/21* root@192.168.1.19:~/40-apache-spark 
scp 40-apache-spark/21* root@192.168.1.20:~/40-apache-spark 
scp 40-apache-spark/21* root@192.168.1.21:~/40-apache-spark
scp 40-apache-spark/21* root@192.168.1.22:~/40-apache-spark
scp 40-apache-spark/21* root@192.168.1.23:~/40-apache-spark
#---------------------------------------------------------------#
