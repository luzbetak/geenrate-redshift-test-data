#!/usr/bin/env python3
"""
Spark Data Generator with Hive Support
Generates synthetic data and handles Hive operations using PySpark.
"""
from __future__ import annotations

import hashlib
import os
import random
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Any, Dict, Optional

import yaml
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DateType, TimestampType

@dataclass
class SparkConfig:
    """Configuration settings for Spark and S3."""
    app_name: str           = "DataGenerator"
    spark_home: str         = "/usr/local/spark-2.4.4-bin-hadoop2.7/"
    warehouse_location: str = str(Path('warehouse').absolute())
    s3_endpoint: str        = "s3.us-west-2.amazonaws.com"
    s3_output_path: str     = "s3a://luzbetak/parquet-13"
    enable_hive: bool       = True
    
    @property
    def access_key(self) -> str:
        """Get AWS access key from environment."""
        return os.environ.get('ACCESS_KEY', '')
    
    @property
    def secret_key(self) -> str:
        """Get AWS secret key from environment."""
        return os.environ.get('SECRET_KEY', '')

class SparkSessionManager:
    """Manages Spark session configuration and initialization."""
    
    def __init__(self, config: SparkConfig):
        self.config = config
        os.environ["SPARK_HOME"] = self.config.spark_home
        self.spark: Optional[SparkSession] = None
    
    def initialize(self) -> SparkSession:
        """Initialize and configure Spark session."""
        builder = SparkSession.builder.appName(self.config.app_name)
        
        if self.config.enable_hive:
            builder = (builder
                      .config("spark.sql.warehouse.dir", self.config.warehouse_location)
                      .config("spark.sql.crossJoin.enabled", "true")
                      .enableHiveSupport())
        
        self.spark = builder.getOrCreate()
        self._configure_s3()
        return self.spark
    
    def _configure_s3(self) -> None:
        """Configure S3 settings for Spark context."""
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
        
        sc = self.spark.sparkContext
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        sc.setSystemProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.access.key", self.config.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.config.secret_key)
        hadoop_conf.set("fs.s3a.endpoint", self.config.s3_endpoint)

class DataGenerator:
    """Handles synthetic data generation using Faker and custom logic."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.faker = Faker()
    
    @staticmethod
    def _generate_hash_id(keyword: str) -> str:
        """Generate a SHA-256 hash ID from a keyword."""
        return hashlib.sha256(keyword.encode('utf-8')).hexdigest()[50:]
    
    def _random_word(self) -> str:
        """Get a random word from system dictionary."""
        with open('/usr/share/dict/words') as file:
            words = file.read().splitlines()
        return random.choice(words)
    
    def _generate_random_value(self, field_type: str, field_name: str, record_num: int) -> Any:
        """Generate a random value based on field type and name."""
        if field_type == "BIGINT":
            return random.randint(100, 1_000)
        elif field_type == "BOOLEAN":
            return random.choice([True, False])
        elif field_type == "DATE":
            return self.faker.date_between(start_date='-10y', end_date='now')
        elif field_type == "DATETIME":
            return self.faker.date_time_between(start_date='-10y', end_date='now')
        else:
            if re.search('(id|ID)', field_name):
                return self._generate_hash_id(self._random_word())
            elif re.search('sob', field_name):
                return str(record_num)
            elif re.search('(name|family)', field_name):
                return self.faker.name()
            return ' '.join(self._random_word() for _ in range(10))

    def generate_batch(self, fields: List[Dict[str, str]], batch_size: int) -> DataFrame:
        """Generate a batch of synthetic data."""
        records = []
        field_names = [f['name'] for f in fields]
        
        for i in range(batch_size):
            record = []
            for field in fields:
                value = self._generate_random_value(
                    field['data_type'],
                    field['name'],
                    i
                )
                record.append(value)
            records.append(record)
        
        rdd = self.spark.sparkContext.parallelize(records, 128)
        return self.spark.createDataFrame(rdd, field_names)

class DataWriter:
    """Handles data writing operations to various formats."""
    
    def __init__(self, spark: SparkSession, config: SparkConfig):
        self.spark = spark
        self.config = config
    
    def write_parquet(self, df: DataFrame, partition_col: Optional[str] = None) -> None:
        """Write DataFrame to Parquet format."""
        writer = df.write
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.parquet(self.config.s3_output_path, mode="append")
    
    def write_csv(self, df: DataFrame, path: str) -> None:
        """Write DataFrame to CSV format."""
        df.write.csv(
            path=path,
            format='csv',
            mode="append",
            sep='\t'
        )

class HiveManager:
    """Manages Hive operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_table(self, table_name: str, schema: StructType) -> None:
        """Create Hive table if it doesn't exist."""
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name}
            USING hive
            AS SELECT * FROM src WHERE 1=0
        """)
    
    def load_data(self, table_name: str, path: str) -> None:
        """Load data into Hive table."""
        self.spark.sql(f"""
            LOAD DATA LOCAL INPATH '{path}'
            INTO TABLE {table_name}
        """)
    
    def query_table(self, table_name: str) -> DataFrame:
        """Query Hive table."""
        return self.spark.sql(f"SELECT * FROM {table_name}")

def main():
    """Main entry point for the application."""
    config = SparkConfig()
    
    # Initialize Spark session
    session_manager = SparkSessionManager(config)
    spark = session_manager.initialize()
    
    try:
        # Initialize components
        generator = DataGenerator(spark)
        writer = DataWriter(spark, config)
        hive_manager = HiveManager(spark)
        
        # Load schema from YAML
        with open("90-ddl/02-test-ddl.yaml") as f:
            schema = yaml.safe_load(f)['data']
        
        # Generate and write data
        for batch in range(10):
            df = generator.generate_batch(schema, batch_size=1000)
            writer.write_parquet(df, partition_col="sob")
            print(f"Completed batch {batch + 1}/10")
        
        # Hive operations example
        hive_manager.create_table("src", StructType([
            StructField("key", IntegerType(), True),
            StructField("value", StringType(), True)
        ]))
        hive_manager.load_data(
            "src",
            f"{config.spark_home}/examples/src/main/resources/kv1.txt"
        )
        result = hive_manager.query_table("src")
        result.show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
