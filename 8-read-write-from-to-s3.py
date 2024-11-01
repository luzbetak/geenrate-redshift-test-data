#!/usr/bin/env python3.7
# ---------------------------------------------------------------------------------------------------#
from __future__ import print_function
from io import BytesIO
from operator import add
from random import random
import os, sys, pyspark
import pandas as pd
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------------------------------#
access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')

# ---------------------------------------------------------------------------------------------------#
# https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
# ---------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"

    sc = pyspark.SparkContext("local[*]")
    sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    # --- Create Spark Session ---#
    spark = SparkSession.builder.appName("DataGen").getOrCreate()

    # --- Read data from local csv ---#
    df = spark.read.format("csv").option("inferSchema", True).option("header", True).load("test-file.csv")
    df.show()
    df.printSchema()

    # --- Write Parquet to S3 ---#
    df.write.parquet("s3a://luzbetak/parquet-007", mode="append")
    # df.write.parquet("s3a://luzbetak/parquet-007", mode="overwrite")

    # --- Read Parquet from S3 ---#
    data = spark.read.parquet("s3a://luzbetak/parquet-007")
    data.show()

# ---------------------------------------------------------------------------------------------------#
