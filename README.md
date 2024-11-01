PySpark Copy Data from S3 to Redshift
=====================================

### Installation
* yum install libffi-devel
* yum install -y xz-devel
* pip3.7 install pylzma
* yum install psmisc

### Copy Parquet from S3 into Redshift
```
COPY listing
FROM 's3://mybucket/data/listings/parquet/'
IAM_ROLE 'arn:aws:iam::0123456789012:role/MyRedshiftRole'
FORMAT AS PARQUET;
```


### Install Python
```
cd /usr/src
wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz
tar xzf Python-3.7.4.tgz
cd Python-3.7.4
./configure --enable-optimizations
make altinstall
```

### Amazon S3
```
s3cmd sync parquet-001 s3://luzbetak
```

