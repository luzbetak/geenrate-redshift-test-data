# Data Processing and Generation System

A comprehensive system for data processing, DDL handling, and synthetic data generation using Apache Spark and AWS S3.

## Table of Contents
- [Overview](#overview)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Components](#components)
- [Usage Examples](#usage-examples)
- [Error Handling](#error-handling)

## Overview

This system consists of several interconnected components:
1. Deployment script for distributing files across servers
2. DDL processing utility for handling database definitions
3. Data generation system for creating synthetic data
4. S3 operations handler for reading/writing data to AWS S3

## System Requirements

- Python 3.7+
- Apache Spark 2.4.4+
- AWS Account with S3 access
- Bash shell environment
- Required Python packages:
```
pyspark
faker
pyyaml
ddlparse
```

## Installation

1. Set up environment variables:
```bash
export ACCESS_KEY='your-aws-access-key'
export SECRET_KEY='your-aws-secret-key'
export SPARK_HOME='/usr/local/spark-2.4.4-bin-hadoop2.7/'
```

2. Install required packages:
```bash
pip install pyspark faker pyyaml ddlparse
```

3. Configure AWS S3 access in your environment.

## Configuration

### Spark Configuration
- Default application name: "DataGenerator"
- S3 endpoint: s3.us-west-2.amazonaws.com
- Warehouse location: ./warehouse (absolute path)

### Server Deployment Configuration
- Spark servers: 192.168.1.10-16
- Application servers: 192.168.1.17-23
- SSH user: root
- Connection timeout: 5 seconds

## Components

### 1. Deployment Script (`1-transformation-pipeline.sh`)
Handles file distribution across multiple servers.

Features:
- Automated file deployment to multiple servers
- Server health checking
- Logging with timestamps
- Error handling and reporting

Usage:
```bash
# Deploy files
./1-transformation-pipeline.sh

# Analyze git repository
./1-transformation-pipeline.sh --analyze
```

### 2. DDL Processor (`2-import-ddl.py`)
Processes DDL files and manages data definitions.

Features:
- DDL to YAML conversion
- Table schema management
- Slice distribution processing
- Comprehensive logging

Classes:
- `TableColumn`: Represents database column definitions
- `TableDefinition`: Manages complete table structures
- `DDLProcessor`: Handles DDL file processing
- `YAMLLoader`: Manages YAML file operations
- `SliceProcessor`: Processes data slice distributions

### 3. Data Generator (`3-generate-data.py`)
Generates synthetic data using PySpark.

Features:
- Configurable data generation
- S3 integration
- Custom schema support
- Batch processing

Key Components:
- `SparkConfig`: Configuration management
- `DataGenerator`: Synthetic data generation
- Schema customization via YAML

### 4. Type-based Data Generator (`4-generate-data-by-type.py`)
Extended data generation with Hive support.

Features:
- Hive integration
- Enhanced type handling
- More sophisticated data generation
- Better batch management

Classes:
- `SparkSessionManager`: Manages Spark sessions
- `DataGenerator`: Handles data generation
- `DataWriter`: Manages data output
- `HiveManager`: Handles Hive operations

### 5. S3 Operations (`5-read-write-from-to-s3.py`)
Manages S3 read/write operations.

Features:
- CSV file reading
- Parquet file handling
- S3 integration
- DataFrame operations

Classes:
- `S3Config`: S3 configuration management
- `SparkS3Manager`: Handles S3 operations

## Usage Examples

### Generate Synthetic Data
```python
from generate_data import DataGenerator, SparkConfig

config = SparkConfig()
generator = DataGenerator(config, "s3a://your-bucket/output")
generator.generate_data(fields, chunks=100, records_per_chunk=1000)
```

### Process DDL Files
```python
from import_ddl import DDLProcessor

processor = DDLProcessor()
processor.process_ddl_file(
    input_path="schema.sql",
    output_path="schema.yaml"
)
```

### S3 Operations
```python
from s3_operations import S3Config, SparkS3Manager

config = S3Config.from_env()
manager = SparkS3Manager(config)
manager.initialize()

# Read CSV
df = manager.read_csv("data.csv")

# Write Parquet
manager.write_parquet(df, "s3a://bucket/path", mode="append")
```

## Error Handling

The system implements comprehensive error handling:
- Logging of all operations
- Graceful failure handling
- Detailed error messages
- Proper resource cleanup
- Transaction rollback where applicable

Common error scenarios:
1. S3 connectivity issues
2. Server unavailability
3. Schema validation failures
4. Data type mismatches
5. Resource constraints

---

## Contributing

Please follow these guidelines when contributing:
1. Follow PEP 8 style guide
2. Add comprehensive docstrings
3. Include type hints
4. Write unit tests for new features
5. Update documentation as needed

