## Introduction

Music Streaming Service Sparkify has attained popularity now. Their song database has been increasing daily while lot of people hearing songs and generating logs. Since we are logging the data, the logs are initially stored in JSON format/files.

We need to process this JSON data and analyze it and transform it into insights. In order to achieve this we try to dump this data into AWS Redshift - Cloud based Datawarehouse.

## Project Description

So we have data which is stored in S3 buckets, and we have to develop ETL script to load the tables in AWS Redshift. Below are individual component details

# S3 Buckets :
# Bucket 1 : Songs and Artists

SONG_DATA='s3://udacity-dend/song_data' Bucket 2 contains has info about actions done by users (such as what song are listening, etc..)
LOG_DATA='s3://udacity-dend/log_data' 
AWS Redshift:
These JSON files we will be moving them into RedShift through Copy operationn. Redshift is used as datawarehouse to store the data which will help us to retrieve data faster.

The data structure would be stored as STAR Schema. The Schema has one fact table surrounded by dimension tables which helps Sparkify to analyze and derive insights about the business that they are having.

# Fact Table: 
SongPlays : Used to store songs played by users and artist information

# Dimension Tables: 
Users, Songs, Artists and Time 

# Steps to build the project

Set up AWS components - Start RedShift cluster with dc2.large using 4 nodes (which I did), IAM roles for S3ReadonlyAccess
Scripts - First run 'create_tables.py' to drop existing and create tables as needed for the datawarehouse and also staging area
Finally Run etl.py to load the AWS RedShift cluster

# Individual Files

create_tables.py - Script used to drop existing tables and create new tables for staging and datawarehouse
sql_queries.py - Contain sql scripts for creation and insertion of data, so basically we have DDL and DML statements written over there
dhw.cfg - Contains configuration information about AWS services which we are using
etl.py - ETL script to execute and dump data from S3 to RedShift datawarehouse
