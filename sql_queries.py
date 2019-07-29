############################################################
# Imports & Instantiates Config Variable and Reads 'dwg.cfg'
############################################################

import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

######################################################
# Drops Table(s) In AWS Redshift Cluster If They Exist
######################################################

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

###############################################################
# Creates Table(s) in AWS Redshift Cluster If They Do Not Exist
###############################################################

staging_events_table_create= ("""CREATE TABLE staging_events(
	event_id INTEGER IDENTITY(0,1),
	artist_name VARCHAR,
	user_auth VARCHAR,
	user_first_name VARCHAR,
	user_gender VARCHAR,
	item_in_session INTEGER,
	user_last_name VARCHAR,
	song_length DOUBLE PRECISION,
	user_level VARCHAR,
	login_location VARCHAR
	request_method VARCHAR,
	page VARCHAR,
	registration VARCHAR,
	session_id BIGINT,
	song_title VARCHAR,
	status INTEGER,
	time_stamp VARCHAR,
	user_agent VARCHAR,
	user_id VARCHAR,
	PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
	song_id VARCHAR,
	num_songs INTEGER,
	artist_id VARCHAR,
	artist_latitude DOUBLE PRECISION,
	artist_longitude DOUBLE PRECISION,
	artist_location VARCHAR,
	artist_name VARCHAR,
	title VARCHAR,
	duration DOUBLE PRECISION,
	year INTEGER,
	PRIMARY KEY (song_id))
""")

songplay_table_create = ("""CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR NOT NULL,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE users(
    user_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE songs(
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")


artist_table_create = ("""CREATE TABLE artists(
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""CREATE TABLE time(
    start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

################################################
# Copies Data From S3 Bucket Into Staging Tables
################################################

staging_events_copy = ("""copy staging_events from {} iam_role {} region 'us-west-2' json {};""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])
staging_songs_copy = ("""copy staging_songs from {} iam_role {} region 'us-west-2' json 'auto';""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

###############################################
# Inserts Data Into AWS Redshift Cluster Tables
###############################################

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT timestamp 'epoch' + SE.time_stamp/1000 * interval '1 second' as start_time, SE.user_id, SE.user_level,
                            SS.song_id, SS.artist_id, SE.session_id, SE.login_location, SE.user_agent
                            FROM staging_events SE, staging_songs SS
			    JOIN SE ON SE.song_title = SS.title
			    JOIN SE ON SE.artist_name = SS.artist_name
                            WHERE SE.page = 'NextSong'""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                        SELECT DISTINCT user_id, user_first_name, user_last_name, user_gender, user_level
                        FROM staging_events WHERE page = 'NextSong'""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          FROM staging_songs""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                        SELECT start_time, EXTRACT(hour from start_time), EXTRACT(day from start_time),
                        EXTRACT(week from start_time), EXTRACT(month from start_time), EXTRACT(year from start_time), EXTRACT(weekday from start_time)
                        FROM songplays""")

##########################################
# List of Queries For AWS Redshift Cluster
##########################################

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
