import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df = 

    # insert song record
    song_data = 
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = import os
import glob
import psycopg2
import pandas as pd
from sqlqueries import *
from datetime import datetime
from createtables import *


    
def getfiles(conn,cur,filepath,func):
    all_files=[]
    for root,dirs,files in os.walk(filepath):
        files=glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    
    num_files=len(all_files)
    print('{} files found in {}'.format(num_files,filepath))
    
    for i, datafile in enumerate(all_files,1):
        func(cur,datafile)
        conn.commit()
        print('{}/{} files processed'.format(i,num_files))
    #return all_files

def process_song_file(cur,filepath):
    df=pd.read_json(filepath,lines=True)
    
    song_data=(df[['song_id','title','artist_id','year','duration']].values).tolist()
    cur.execute(song_table_insert, song_data[0])
    
    artist_data=(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values).tolist()
    cur.execute(artist_table_insert,artist_data[0])

def process_log_file(cur,filepath):
    df=pd.read_json(filepath,lines=True)
    df=df[df['page']=='NextSong']
    
    t=df['ts'].apply(lambda x: datetime.fromtimestamp(x/1000))
    
    column_labels=['time','hour','day','week','month','year','weekday']
    
    df2=pd.DataFrame(columns=column_labels)

    df2['time']=t
    df2['hour']=t.dt.hour
    df2['day']=t.dt.day
    df2['week']=t.dt.week
    df2['month']=t.dt.month
    df2['year']=t.dt.year
    df2['weekday']=t.dt.weekday

    time_df=df2
    
    for i,row in time_df.iterrows():
        cur.execute(time_table_insert,list(row))
    #conn.commit()

        user_df1=df[['userId','firstName','lastName','gender','level']]

        user_df=user_df1

    for i,row in user_df.iterrows():
        cur.execute(user_table_insert,list(row))
        #conn.commit()
        
    for index,row in df.iterrows():
        cur.execute(song_select,(row.song,row.artist,row.length))
        results=cur.fetchone()
    
        #print(results)
        if results:
            song_id,artist_id=results
        else:
            song_id,artist_id=None,None
    
        songplay_data=(pd.to_datetime(row.ts,unit='ms'),int(row.userId),row.level,song_id,artist_id,row.sessionId,row.location,row.userAgent)
        

    #songplay_data=(row.level)
    cur.execute(songplay_table_insert,songplay_data)   
    #conn.commit()
    
    
    
    
    


def main():
    conn=psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
    cur=conn.cursor()
    getfiles(conn,cur,filepath='data/song_data',func=process_song_file)
    getfiles(conn,cur,filepath='data/log_data',func=process_log_file)
    conn.close()

if __name__ == "__main__":
    main()


    

    
        
        
   
        
    
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = 

    # filter by NextSong action
    df = 

    # convert timestamp column to datetime
    t = 
    
    # insert time data records
    time_data = 
    column_labels = 
    time_df = 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()