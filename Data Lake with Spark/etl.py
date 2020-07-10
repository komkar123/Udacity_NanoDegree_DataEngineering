"""
Importing the libraries, packages and  functions necessary for the project

"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql import SQLContext
#from pyspark.sql.types import 
from pyspark.sql.dataframe import *
from datetime import datetime
#from pyspark.sql.functions import timestamp
from pyspark.sql import types as T


"""
Configured the files required for AWS authentication
"""

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']

"""
Function to create the spark session for performing analytics
"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
Function for processing the song data and loading song and artist tables
"""
def process_song_data(spark, input_data, output_data):
    # getting filepath to song data file
    song_data = "/song_data"
    
    # reading song data file
    df = spark.read.format("json").load(os.path.join(input_data,song_data))
    #df = spark.read.format("json").load("s3a://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json")
    
    #creating the view for songs table
    df.createOrReplaceTempView("songs_table")
    
   
    # extracting distinct columns to create songs table
    songs_table = spark.sql("""
                            select distinct song_id,title,artist_id,year,duration from songs_table
                            where song_id IS NOT NULL
                            """)
    
    
    # writing songs table to parquet files partitioned by year and artist 
    songs_table = songs_table.write.partitionBy("year","artist_id").parquet(os.path.join(output_data,"songs"),"overwrite")

    # extracting columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude")
    
    # writing artists table to parquet files
    artists_table = artists_table.write.parquet(os.path.join(output_data,"artists"),"overwrite")

"""
Function for processing the log files and loading time users and songplays tables
"""
def process_log_data(spark, input_data, output_data):
    # getting filepath to log data file
    
    log_data = "/log_data"

    # reading log data file
    df = spark.read.format("json").load(os.path.join(input_data,log_data))
    
    #log_data="s3a://udacity-dend/log_data/2018/11/*"

    #df=spark.read.format("json").load(log_data)
    
    # filtering by actions for song plays (NExt Page)
    df = df.filter("Page=='NextSong'")
    
    #creating a view to extract distinct users
    df.createOrReplaceTempView("logs_table")
    
    # extracting columns for users table    
    artists_table = spark.sql("""
                              select distinct userId, firstName, lastName, gender
                              from logs_table 
                              where userId IS NOT NULL
                              """)
    
    # writing users table to parquet files
    artists_table = artists_table.write.parquet(os.path.join(output_data,"users"),"overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    dfts = df.withColumn("ts",get_timestamp("ts"))
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    dfdatetime = df.withColumn("ts",get_datetime("ts"))
    
    # extracting columns to create time table
    time_table = dfts.select(
                    date_format('ts','h:m:s a ').alias("start_time"),
                    hour('ts').alias('hour'),
                    dayofmonth('ts').alias('day'),
                    weekofyear('ts').alias('week'),
                    month('ts').alias('month'),
                    year('ts').alias('year'),
                    date_format('ts','E').alias('weekday'))
    
    # writing time table to parquet files partitioned by year and month
    time_table = time_table.write.partitionBy("year","month").parquet(os.path.join(output_data,"/time"),"overwrite")

    # reading in song data to use for songplays table
    song_df = spark.read.format("json").load(os.path.join(input_data,"song_data"))
    #song_df = spark.read.format("json").load("s3a://udacity-dend/song_data/A/A/A/TRAAAAK128F9318786.json")
    
    #function for getting start_time
    timefunc1=udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%H:%M:%S'))
    
    
    # extracting columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(df,(song_df.title==df.song) & (df.artist==song_df.artist_name) ).select( timefunc1('ts').alias('start_time'),"userId","level","song_id","artist_id","sessionId","location","userAgent",month(timefunc1('ts')).alias('month'),year(timefunc1('ts')).alias('year'))
    
    songplays=songplays_table.withColumn("songplay_id",monotonically_increasing_id())

    # writing songplays table to parquet files partitioned by year and month
    songplays_table = songplays.write.partitionBy("year","month").parquet(os.path.join(output_data,"songplays"),"overwrite")

"""
Main function to start the execution of the project
"""
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-omkar1/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
