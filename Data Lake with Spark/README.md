## Data Lake Project

# Project Description

The Sparikfy wants to analyze and get insights of the users who are listening to songs. For this, we are supposed to develop ETL pipeline to extract song and log data from the S3 hosted buckets by Udacity. We then have to perform analytics using Spark through Python (PySpark) and load the target dimension and fact tables

We need to process this JSON data and analyze it and transform it into insights. In order to achieve this we try to dump this data back into S3 which can be visualized by many BI tools on AWS.


# S3 Buckets :
# Bucket 1 : Songs and Artists

SONG_DATA='s3://udacity-dend/song_data' Bucket 2 contains has info about actions done by users (such as what song are listening, etc..)
LOG_DATA='s3://udacity-dend/log_data' 

# Bucket 2 : Target S3 buckey

s3a://udacity-omkar1/

# Individual Files

dhw.cfg - Contains configuration information about AWS services which we are using
etl.py - ETL script to execute and process data using spark and dump data back to S3 as parquet files

# Steps

1. Install spark on the workspace (Prerequisites)
2. Enter AWS Access ID and key in the config file
3. Run the etl.py script
4. Check target S3 location for the resulting tables as parquet files

