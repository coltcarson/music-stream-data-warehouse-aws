#################################
# Imports Plugins and sql_queries
#################################

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

###########################################################
# Copies Data from AWS S3 Buckets into AWS Redshift Cluster
###########################################################

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
#################################################
# Inserts Data into AWS Redshift Cluster Table(s)
#################################################

def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
##################################################################################
# Connects to AWS Redshift Cluster and Calls load_staging_tables and insert_tables
##################################################################################

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
