# Music Streaming Data Warehouse (S3 and Redshift)

For this project, we created a music streaming data warehouse and ETL pipeline using S3, Redshift, and Python. We began by using Python to extracting user listening and song metadata JSON files from S3 buckets so that it could be staged within Redshift. Once staged, we transformed the data so it may be stored in dimension tables for future analytics.

--------------------------------------------

#Schema
![Redshift Schema](https://github.com/coltcarson/music-stream-data-warehouse-aws/blob/master/Udacity%20DE%20Project%20%233-2.png)
