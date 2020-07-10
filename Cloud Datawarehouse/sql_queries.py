import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE=config['IAM_ROLE']['ARN']
LOG_DATA=config['S3']['LOG_DATA']
SONG_DATA=config['S3']['SONG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']


# DROP TABLES

staging_events_table_drop = "drop table if exists st_events"
staging_songs_table_drop = "drop table if exists st_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""create table if not exists st_events(
artist text,
auth text,
firstname text,
gender char(1),
iteminSession int,
lastname text,
length numeric,
level text,
location text,
method text,
page text,
registration numeric,
sessionid int,
song text,
status text,
ts bigint,
useragent text,
userid int
)
""")

staging_songs_table_create = (""" create table if not exists st_songs (
num_songs int,
artist_id varchar,
artist_lattitude numeric,
artist_longitude numeric,
location text,
name text,
song_id text,
title text,
duration numeric,
year int
)
""")

songplay_table_create = ("""create table if not exists songplays 
(songplay_id integer identity(0,1) primary key, 
start_time timestamp ,
user_id int ,
level varchar(5) , 
song_id varchar(25) ,
artist_id varchar(25) , 
session_id int, 
location varchar(70),
user_agent varchar(200))

""")

user_table_create = ("""create table  if not exists users 
(user_id int primary key,
first_name varchar(15),
last_name varchar(15),
gender char(1), 
level varchar(10))
""")

song_table_create = ("""create table  if not exists songs 
(song_id varchar(25) primary key, 
title varchar(100),
artist_id varchar(25) ,
year int, 
duration numeric
)
""")

artist_table_create = ("""create table  if not exists artists 
(artist_id varchar(25) primary key,
name varchar(100), 
location varchar(100),
latitude float(5), 
longitude float(5))
""")

time_table_create = ("""create table if not exists time 
(start_time timestamp primary key,
hour int, 
day int,
week int,
month int,
year int,
weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
copy st_events 
from {}
iam_role {}
region 'us-west-2'
json 'auto';
""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
copy st_songs 
from {}
iam_role {}
region 'us-west-2'
json 'auto';
""").format(SONG_DATA,IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  distinct timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.userid, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM st_events se, st_songs ss
WHERE se.page = 'NextSong' AND
se.song =ss.title AND
se.artist = ss.name AND
se.length = ss.duration
""")

user_table_insert = ("""
insert into users (user_id,first_name,last_name,gender,level) \
(select distinct userId,firstName,lastName,gender,level from st_events where page='NextSong');
""")

song_table_insert = ("""
insert into songs (song_id,title,artist_id,year,duration) \
(select distinct song_id,title,artist_id, year, duration from st_songs);
""")

artist_table_insert = ("""
insert into artists (artist_id,name,location,latitude,longitude) \
(select distinct artist_id,name,location,artist_lattitude,artist_longitude from st_songs where artist_id IS NOT NULL);
""")

time_table_insert =("""INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT distinct start_time, extract(hour from start_time), extract(day from start_time),
extract(week from start_time), extract(month from start_time),
extract(year from start_time), extract(dayofweek from start_time)
FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
