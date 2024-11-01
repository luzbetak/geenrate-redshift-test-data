#!/usr/bin/env python3
"""
Data Generator Script
Generates synthetic data using PySpark and saves it to S3 in Parquet format.
"""
from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import List, Any

import yaml
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    DateType,
)

@dataclass
class SparkConfig:
    """Configuration settings for Spark session."""
    app_name: str = "DataGenerator"
    s3_endpoint: str = "s3.us-west-2.amazonaws.com"
    spark_home: str = "/usr/local/spark-2.4.4-bin-hadoop2.7/"
    
    @property
    def access_key(self) -> str:
        return os.environ.get('ACCESS_KEY', '')
    
    @property
    def secret_key(self) -> str:
        return os.environ.get('SECRET_KEY', '')

class DataGenerator:
    """Handles generation of synthetic data using PySpark."""
    
    def __init__(self, config: SparkConfig, output_path: str):
        self.config = config
        self.output_path = output_path
        self.faker = Faker()
        self.spark = self._initialize_spark()
        
    def _initialize_spark(self) -> SparkSession:
        """Initialize and configure Spark session with S3 settings."""
        os.environ["SPARK_HOME"] = self.config.spark_home
        
        spark = (SparkSession.builder
                .appName(self.config.app_name)
                .getOrCreate())
        
        # Configure S3 settings
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.access.key", self.config.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.config.secret_key)
        hadoop_conf.set("fs.s3a.endpoint", self.config.s3_endpoint)
        
        return spark

    @staticmethod
    def _generate_hash_id(keyword: str) -> str:
        """Generate a SHA-256 hash ID from a keyword."""
        return hashlib.sha256(keyword.encode('utf-8')).hexdigest()[50:]

    @staticmethod
    def _random_date(start_year: int = 2013) -> datetime:
        """Generate a random date between start_year and now."""
        start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
        end = datetime.now(timezone.utc)
        days_between = (end - start).days
        random_days = timedelta(days=random.randint(0, days_between))
        return start + random_days

    @staticmethod
    def _random_word() -> str:
        """Get a random word from the system dictionary."""
        with open('/usr/share/dict/words') as file:
            words = file.read().splitlines()
        return random.choice(words)

    @classmethod
    def load_schema_from_yaml(cls, yaml_path: str) -> List[str]:
        """Load and parse the data schema from a YAML file."""
        with open(yaml_path) as file:
            schema = yaml.safe_load(file)
        
        return [
            f"{field['name']}|{field['data_type']}|{field.get('length', '')}"
            for field in schema.get("data", [])
        ]

    def _get_spark_schema(self) -> StructType:
        """Define the Spark schema for the generated data."""
        return StructType([
            StructField("Col1", StringType(),  True),
            StructField("Col2", StringType(),  True),
            StructField("Col3", StringType(),  True),
            StructField("Col4", BooleanType(), True),
            StructField("Col5", StringType(),  True),
            StructField("Col6", IntegerType(), True),
            StructField("Col7", StringType(),  False),
            StructField("Col8", DateType(),    True),
            StructField("Col9", StringType(),  True)
        ])

    def _generate_record(self, field_def: str, record_num: int) -> Any:
        """Generate a single field value based on its definition."""
        name, data_type, _ = field_def.split('|')
        
        if data_type == "BIGINT":
            return random.randint(1, 100)
        elif data_type == "BOOLEAN":
            return random.choice([True, False])
        elif data_type == "DATE":
            return self.faker.date_between(start_date='-10y', end_date='now')
        elif data_type == "DATETIME":
            return self.faker.date_time_between(start_date='-10y', end_date='now')
        elif any(pattern in name.lower() for pattern in ['id', 'ID']):
            return self._generate_hash_id(self._random_word())
        elif 'sob' in name.lower():
            return str(record_num)
        elif any(pattern in name.lower() for pattern in ['name', 'family']):
            return self.faker.name()
        else:
            return self._random_word()

    def generate_data(self, fields: List[str], chunks: int = 100, records_per_chunk: int = 1000):
        """Generate synthetic data and save to Parquet format."""
        schema = self._get_spark_schema()
        
        for chunk in range(chunks):
            records = []
            for record_num in range(records_per_chunk):
                record = [self._generate_record(field, record_num) for field in fields]
                records.append(record)
            
            df = self.spark.createDataFrame(records, schema)
            df.write.partitionBy("Col1").parquet(self.output_path, mode="append")
            
            print(f"Completed chunk {chunk + 1}/{chunks}")

def main():
    """Main entry point for the data generation script."""
    import random
    
    config = SparkConfig()
    generator = DataGenerator(
        config=config,
        output_path="s3a://luzbetak/parquet-14"
    )
    
    fields = generator.load_schema_from_yaml("90-Data-Definition-Language/2-test-ddl.yaml")
    generator.generate_data(fields)
    generator.spark.stop()

if __name__ == "__main__":
    main()
