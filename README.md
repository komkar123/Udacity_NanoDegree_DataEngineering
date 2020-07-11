# Data Engineering Nanodegree

## Projects for the nanodegree offered by Udacity

#### [Data Modeling with Postgres] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Data%20Modeling%20with%20Postgres) 

In this project, we have to model data with Postgres and build and ETL pipeline using Python to perform optimized queries on song play analysis. On the database side, We have to define fact and dimension tables to use Star Schema and ETL pipeline should be created to transfer data from files located in two local directories into these tables in Postgres using Python and SQL.

#### [Data Modeling with Cassandra] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Data%20Modeling%20with%20Cassandra)

Data Modeling is made through Apache Cassandra while developing a ETL script which loads the data using Python scripting. ETL script dumps the data from csv file to a star schema based datawarehouse stored in NoSQL. THe database design is done prior to writing ETL script so we have right design before developing workflows.

### [Building Cloud Datawarehouse] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Cloud%20Datawarehouse)

Music Streaming Service Sparkify has attained popularity now. Their song database has been increasing daily while lot of people hearing songs and generating logs. Since we are logging the data, the logs are initially stored in JSON format/files. We need to process this JSON data and analyze it and transform it into insights. In order to achieve this we try to dump this data into AWS Redshift - Cloud based Datawarehouse.

### [Building Data Lake with Spark] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Data%20Lake%20with%20Spark)

The Sparikfy wants to analyze and get insights of the users who are listening to songs. For this, we are supposed to develop ETL pipeline to extract song and log data from the S3 hosted buckets by Udacity. We then have to perform analytics using Spark through Python (PySpark) and load the target dimension and fact tables. We need to process this JSON data and analyze it and transform it into insights. In order to achieve this we try to dump this data back into S3 which can be visualized by many BI tools on AWS.

### [Processing ETL Pipelines with Apache Airflow] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Airflow)

In this project, we use Airflow to manage, develop and monitor the etl pipelines. By using different Operators, Hooks for connecting to AWS on user's behalf allows us to create dump the data from JSON files into redshift. We define DAGs (Directed Acyclic Graph) to create tables, insert data into tables and move data from S3 bucket to redshift. The use of Airflow is made to monitor and schedule the pipelines and it is easy to view logs and see and manage the errors.

### [Capstone Project - Modeling Immigration and Temperature of US Cities] (https://github.com/komkar123/Udacity_NanoDegree_DataEngineering/tree/master/Capstone%20project)

Currently the immigration data is being looked up as a fundamental dataset here. We also have temperature data around all the cities of the world. Using these datasets, I have decided to model the data based on location, time and immigration data to get average temperature of the designated city. The dataframes are processed using spark and aggregration operations are performed accordingly. The dimensional modeling is done and the target tables are partitioned and written to parquet files.
