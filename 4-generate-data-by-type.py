#!/usr/bin/env python3.7
# ---------------------------------------------------------------------------------------------------#
# ---------------------------------------------------------------------------------------------------#
from __future__ import print_function
from os.path import expanduser, join, abspath
from faker import Faker
import pprint
import random, os, sys, pyspark
from datetime import timedelta, timezone, datetime

import hashlib
import re
import yaml
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import DataFrameWriter
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

pp = pprint.PrettyPrinter(width=41, compact=True)
# ---------------------------------------------------------------------------------------------------#
filename = "s3a://luzbetak/parquet-13"

# ---------------------------------------------------------------------------------------------------#
access_key = os.environ.get('ACCESS_KEY')
secret_key = os.environ.get('SECRET_KEY')

# ---------------------------------------------------------------------------------------------------#
# os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.7"
os.environ["SPARK_HOME"] = "/usr/local/spark-2.4.4-bin-hadoop2.7/"
# os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.7 pyspark-shell"

warehouse_location = abspath('warehouse')

# --- Create Spark Session, Context and SQL ---#
spark = SparkSession.builder.appName("DataGenerator").config("spark.sql.crossJoin.enabled", "true").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("DataGenerator") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
# sc.setSystemProperty("spark.python.worker.memory", "4g")
# sc.setSystemProperty("spark.driver.cores", "4")
# sc.setSystemProperty("spark.driver.memory", "4g")
# sc.setSystemProperty("spark.executor.memory", "1g")
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# sqlContext = SQLContext(sc)

# --- Amazon S3 Configuration ---#
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

# ---------------------------------------------------------------------------------------------------#
def random_date():
    step = timedelta(days=1)
    start = datetime(2013, 1, 1, tzinfo=timezone.utc)
    end = datetime.now(timezone.utc)
    random_date = start + random.randrange((end - start) // step + 1) * step

    return random_date


# ---------------------------------------------------------------------------------------------------#
def random_sentence(words=10):
    sentence = ''
    lines = open('/usr/share/dict/words').read().splitlines()

    for a in range(words):
        line = random.choice(lines)
        sentence = sentence + " " + line

    return sentence


# ---------------------------------------------------------------------------------------------------#
def load_yaml_ddl(yaml_ddl):
    with open(yaml_ddl) as file:
        documents = yaml.full_load(file)

    # pp.pprint(documents)

    # v = documents.items()
    # pp.pprint(v)

    # -------------------------------------- #
    #        Display Field List
    # -------------------------------------- #
    # field_list = documents.get("data")
    # for i in field_list:
    #     print(i.get("name"))

    # for item, doc in documents.items():
    # print(item, ":", doc)
    #    pp.pprint(doc)

    field_array = []
    for i in documents.get("data"):
        field_array.append(i.get("name") + "|" + i.get("data_type") + "|" + str(i.get("length")))

    return field_array


# -------------------------------------------------------------------------------------------------- #
def get_fake_id(keyword):
    simple256 = hashlib.sha256(keyword.encode('utf-8')).hexdigest()
    return simple256[50:]


# ---------------------------------------------------------------------------------------------------#
# Prototype to generate sample data: 
#    1. Generate client synthetic data into a list by appending the list
#    2. Parallelize list into Resilient Distributed Datasets (RDD)
#    3. Create DataFrame from the RDD
#    4. Save the DataFrame incrementally into S3 parquet format
#    5. Repeat 100 million times from 100+ servers running concurrently
#    6. Copy parquet format into Redshift Cluster
# ---------------------------------------------------------------------------------------------------#
# https://spark.apache.org/docs/latest/configuration.html
# ---------------------------------------------------------------------------------------------------#
def generate_sample_data(records):

    # --- Dynamically build set of records --- #
    faker = Faker()
    field_name = []
    for y in range(0, 10):
        for x in range(0, 1000):

            list1 = []  # list1 = [('0', '0', 0, 0), ('0', '0', 0, 0)]
            temp = []
            for rec in records:
                array = rec.split('|')

                # track field names only once
                if x == 0:
                    field_name.append(array[0])

                if array[1] == "BIGINT":
                    temp.append(random.randint(100, 1_000))
                elif array[1] == "BOOLEAN":
                    temp.append(random.choice([True, False]))
                elif array[1] == "DATE":
                    temp.append(faker.date_between(start_date='-10y', end_date='now'))
                elif array[1] == "DATETIME":
                    temp.append(faker.date_time_between(start_date='-10y', end_date='now'))
                else:
                    if re.search('(id|ID)', array[0]):
                        temp.append(get_fake_id(random_sentence(1)))
                    elif re.search('sob', array[0]):
                        temp.append(str(x))
                    elif re.search('(name|family)', array[0]):
                        temp.append(faker.name())
                    else:
                        temp.append(random_sentence(1))

            list1.append(temp)

        print("-" * 80)
        pp.pprint(field_name)
        print("-" * 80)
        pp.pprint(list1)
        print("-" * 80)

        # val myrdd1 = sc.parallelize(1 to 1000, 15)
        # myrdd1.partitions.length
        # val myrdd2 = myrdd1.coalesce(5,false)
        # myrdd2.partitions.length
        # Int = 5
 
        rdd = sc.parallelize(list1, 128)
        print("-" * 80)
        print("Partition Lenght: " + str(rdd.getNumPartitions()) )
        print("-" * 80)
        df = spark.createDataFrame(rdd, field_name)
        
        # df.collect()
        # df.show(25, False)
        # df.printSchema()

        # --- Write Parquet to S3 ---#
        # df.write.partitionBy("sob").parquet(filename, mode="append")
        df.write.csv(path="/root/data.csv", format='csv', mode="append" , sep='\t')
        # df.write.save(path='csv', format='csv', mode='append', sep='\t')


        # df.write.parquet(filename, mode="append") 
        # df.write.parquet(filename, mode="overwrite")

    # --- Read Parquet from S3 ---#
    # data = spark.read.parquet(filename)
    # data.show(25, False)
    # ------------ End of the Chunk Generation ---------------------------------#

    # print(sc._conf.getAll())
    # SparkContext.stop(sc)


# ---------------------------------------------------------------------------------------------------#
def load_hive():

    # spark is an existing SparkSession
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    spark.sql("LOAD DATA LOCAL INPATH '/usr/local/spark-2.4.4-bin-hadoop2.7/examples/src/main/resources/kv1.txt' INTO TABLE src")
    
    # Queries are expressed in HiveQL
    spark.sql("SELECT * FROM src").show()

# ---------------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    # generate_sample_data()
    
    # fields = load_yaml_ddl("90-ddl/02-test-ddl.yaml")
    # print(fields)
    # print(len(fields))
    # generate_sample_data(fields)

    load_hive()

    spark.stop()
# ---------------------------------------------------------------------------------------------------#
