import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#Drop tables
def drop_tables(cur, conn):
"""
This will drop the tables if they are existing to avoid future errors
"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

#create tables
def create_tables(cur, conn):
"""
This will create new tables for our ETL script to dump the data into
"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

#Beginning of the script
def main():
"""
This execute first, it will configure the aws credentials and connect to the database and execute create and drop functions
"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()