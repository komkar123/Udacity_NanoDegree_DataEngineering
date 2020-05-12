import psycopg2
from sqlqueries import drop_queries, create_queries

def createdatabase():
    conn=psycopg2.connect('host=127.0.0.1 dbname=studentdb user=student password=student')
    
    conn.set_session(autocommit=True)
    cur=conn.cursor()
    
    cur.execute("drop database if exists sparkifydb")
    cur.execute("create database sparkifydb with encoding 'utf8' template template0")
    
    conn.close()
    
    conn=psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=student password=student')
    cur=conn.cursor()
    
    return cur,conn

def drop_tables(cur,conn):
    for query in drop_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur,conn): 
    for query in create_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    cur,conn=createdatabase()
    drop_tables(cur,conn)
    create_tables(cur,conn)
    conn.close()

if __name__=="__main__":
    main()
