# Music Streaming Data Warehouse (S3 and Redshift)

For this project, we created a music streaming data warehouse and ETL pipeline using S3, Redshift, and Python. We began by using Python to extracting user listening and song metadata JSON files from S3 buckets so that it could be staged within Redshift. Once staged, we transformed the data so it may be stored in dimension tables for future analytics.

--------------------------------------------

# Schema
![Redshift Schema](https://github.com/coltcarson/music-stream-data-warehouse-aws/blob/master/Udacity%20DE%20Project%20%233-2.png)

--------------------------------------------

# Instructions
1) Create an AWS Redshift Cluster (dc2.large - 4 Nodes) and add the required login credentials to the dwh.cfg file.

2) Type run create_tables.py in your terminal to drop any extraneous tables and to add new tables to the cluster.

3) Type run etl.py in your terminal to copy the data from S3 into your staging tables which will then transform and insert the data into the dimension tables.  
