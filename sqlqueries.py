drop_song_play="drop table if exists songplays"
drop_users="drop table if exists users"
drop_songs="drop table if exists songs"
drop_artists="drop table if exists artists"
drop_time="drop table if exists time"

#create tables


users=("""create table  if not exists users (user_id int primary key, first_name varchar(15), last_name varchar(15), gender char(1), level varchar(10))
""")

songs=("""create table  if not exists songs (song_id varchar(25) primary key, title varchar(100), artist_id varchar(25) , year int, duration numeric)""")

artists=("""create table  if not exists artists (artist_id varchar(25) primary key, name varchar(100), location varchar(100), latitude float(5), longitude float(5))""")

time =("""create table   if not exists time (start_time timestamp primary key, hour int, day int, week int, month int, year int, weekday int)""")


song_play_create=("""create table if not exists songplays (songplay_id serial primary key not null, start_time timestamp , user_id int , level varchar(5) , song_id varchar(25) , artist_id varchar(25) , session_id int, location varchar(70), user_agent varchar(200))""")

song_table_insert="""insert into songs (song_id,title,artist_id,year,duration) \
values (%s,%s,%s,%s,%s) \
on conflict(song_id) do nothing;"""

artist_table_insert="""insert into artists (artist_id,name,location,latitude,longitude) \
values (%s,%s,%s,%s,%s) \
on conflict(artist_id) do nothing;"""

time_table_insert="""insert into time (start_time,hour,day,week,month,year,weekday) \
values (%s,%s,%s,%s,%s,%s,%s) \
on conflict(start_time) do nothing;"""

user_table_insert="""insert into users (user_id,first_name,last_name,gender,level)\
values (%s,%s,%s,%s,%s) \
on conflict(user_id) do update set level=excluded.level;"""

song_select = ("""select songs.song_id,songs.artist_id from songs join artists on songs.artist_id=artists.artist_id where (title=%s and name=%s and duration=%s) """)
              
songplay_table_insert = """insert into songplays (songplay_id ,start_time , user_id , level , song_id, artist_id , session_id , location , user_agent) values \
(%s,%s,%s,%s,%s,%s,%s,%s,%s) """

#songplay_table_insert = """insert into songplays ( songlevel ) values \
#(%s,) on conflict (songplay_id) do nothing"""

drop_queries=[drop_song_play,drop_users,drop_songs,drop_artists,drop_time]

create_queries=[users,artists,songs,time,song_play_create]



