import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
"""
Loading the data into staging tables from S3 bucket
"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
"""
Inserting the data into staging tables  using the scripts in sql_queries
""" 
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
"""
Firstly, we log configs from dwh.cfg and then start loading data from S3 to tables created (Staging tables) and simultaneously load them into fact and dimesion tables
"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()