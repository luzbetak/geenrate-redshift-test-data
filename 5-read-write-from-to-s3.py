#!/usr/bin/env python3
"""
S3 PySpark Operations
Handles reading from CSV files and reading/writing Parquet files to/from S3.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyspark
from pyspark.sql import SparkSession, DataFrame


@dataclass
class S3Config:
    """Configuration settings for S3 and Spark."""
    access_key: str
    secret_key: str
    region: str = "us-west-2"
    endpoint: str = "s3.amazonaws.com"
    
    @classmethod
    def from_env(cls) -> "S3Config":
        """Create S3Config from environment variables."""
        access_key = os.environ.get('ACCESS_KEY')
        secret_key = os.environ.get('SECRET_KEY')
        
        if not access_key or not secret_key:
            raise ValueError("AWS credentials not found in environment variables")
        
        return cls(
            access_key=access_key,
            secret_key=secret_key
        )
    
    @property
    def endpoint_url(self) -> str:
        """Get the full S3 endpoint URL."""
        return f"s3.{self.region}.{self.endpoint}"


class SparkS3Manager:
    """Manages Spark operations with S3 integration."""
    
    def __init__(self, config: S3Config, app_name: str = "S3DataOperations"):
        """
        Initialize the Spark S3 Manager.
        
        Args:
            config: S3 configuration settings
            app_name: Name of the Spark application
        """
        self.config = config
        self.app_name = app_name
        self.spark: Optional[SparkSession] = None
        
    def initialize(self) -> None:
        """Initialize Spark session with S3 configuration."""
        # Set Hadoop AWS package
        os.environ['PYSPARK_SUBMIT_ARGS'] = (
            "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
        )
        
        # Create Spark context
        sc = pyspark.SparkContext("local[*]")
        sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
        
        # Configure Hadoop for S3
        hadoop_conf = sc._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("com.amazonaws.services.s3.enableV4", "true")
        hadoop_conf.set("fs.s3a.access.key", self.config.access_key)
        hadoop_conf.set("fs.s3a.secret.key", self.config.secret_key)
        hadoop_conf.set("fs.s3a.endpoint", self.config.endpoint_url)
        
        # Create Spark session
        self.spark = SparkSession.builder.appName(self.app_name).getOrCreate()
    
    def read_csv(self, file_path: str | Path) -> DataFrame:
        """
        Read data from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            DataFrame containing the CSV data
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
            
        return (self.spark.read.format("csv")
                .option("inferSchema", True)
                .option("header", True)
                .load(str(file_path)))
    
    def write_parquet(
        self,
        df: DataFrame,
        s3_path: str,
        mode: str = "append"
    ) -> None:
        """
        Write DataFrame to S3 in Parquet format.
        
        Args:
            df: DataFrame to write
            s3_path: S3 path where to write the data
            mode: Write mode ('append' or 'overwrite')
        """
        if mode not in ["append", "overwrite"]:
            raise ValueError("Mode must be either 'append' or 'overwrite'")
            
        df.write.parquet(s3_path, mode=mode)
    
    def read_parquet(self, s3_path: str) -> DataFrame:
        """
        Read Parquet data from S3.
        
        Args:
            s3_path: S3 path to read from
            
        Returns:
            DataFrame containing the Parquet data
        """
        if not self.spark:
            raise RuntimeError("Spark session not initialized")
            
        return self.spark.read.parquet(s3_path)
    
    def display_dataframe_info(self, df: DataFrame, name: str = "DataFrame") -> None:
        """
        Display DataFrame information.
        
        Args:
            df: DataFrame to display
            name: Name to show in the output
        """
        print(f"\n{'-' * 40}")
        print(f"{name} Content:")
        df.show()
        print(f"\n{name} Schema:")
        df.printSchema()
        print(f"{'-' * 40}\n")
    
    def cleanup(self) -> None:
        """Clean up Spark session."""
        if self.spark:
            self.spark.stop()
            self.spark = None


def main() -> None:
    """Main entry point for the application."""
    try:
        # Initialize configuration and Spark manager
        config = S3Config.from_env()
        spark_manager = SparkS3Manager(config)
        spark_manager.initialize()
        
        # Read local CSV file
        csv_df = spark_manager.read_csv("test-file.csv")
        spark_manager.display_dataframe_info(csv_df, "CSV Data")
        
        # Write to S3 as Parquet
        s3_path = "s3a://luzbetak/parquet-007"
        spark_manager.write_parquet(csv_df, s3_path, mode="append")
        
        # Read back from S3
        parquet_df = spark_manager.read_parquet(s3_path)
        spark_manager.display_dataframe_info(parquet_df, "Parquet Data")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        raise
    
    finally:
        # Ensure Spark session is properly cleaned up
        if 'spark_manager' in locals():
            spark_manager.cleanup()


if __name__ == "__main__":
    main()
