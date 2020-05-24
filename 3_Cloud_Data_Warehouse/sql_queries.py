import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN=config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH=config['S3']['LOG_JSONPATH']
SONG_DATA=config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#This staging table contains all raw data
staging_events_table_create= ("""
    CREATE TABLE staging_events(
    artist varchar NULL,
    auth varchar NULL,
    firstname varchar NULL,
    gender varchar NULL,
    iteminsession varchar NULL,
    lastname varchar NULL,
    length varchar NULL,
    level varchar NULL,
    location varchar NULL,
    method varchar NULL,
    page varchar NULL,
    registration varchar NULL,
    session_id int NOT NULL SORTKEY DISTKEY,
    song varchar NULL,
    status varchar NULL,
    ts BIGINT NOT NULL,
    user_agent varchar NULL,
    user_id int NULL
    );
""")

#Another staging table
staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
    num_songs int NULL,
    artist_id varchar NOT NULL SORTKEY DISTKEY,
    artist_latitude varchar NULL,
    artist_longitude varchar NULL,
    artist_location varchar NULL,
    artist_name varchar NULL,
    song_id varchar NOT NULL,
    title varchar NULL,
    duration float,
    year INT NULL  
    );
""")

#Fact table
songplay_table_create = ("""
    CREATE TABLE songplay(
    songplay_id  INT IDENTITY(0, 1) NOT NULL SORTKEY, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL DISTKEY, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar
    )
""")

#users dimension table
user_table_create = ("""
    CREATE TABLE users(
    user_id INT NOT NULL SORTKEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar, 
    level varchar
    ) diststyle all;
""")

#songs dimension table
song_table_create = ("""
    CREATE TABLE songs(
    song_id varchar NOT NULL SORTKEY, 
    title varchar, 
    artist_id varchar NOT NULL, 
    year int, 
    duration float
    )
""")

#artists dimension table
artist_table_create = ("""
    CREATE TABLE artists(
    artist_id varchar NOT NULL SORTKEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float
    )
""")

#time dimension table
time_table_create = ("""
    CREATE TABLE time(
    start_time timestamp PRIMARY KEY,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
    )
""")

# STAGING TABLES
#Copy data from json file specified in config file
staging_events_copy = ("""
COPY staging_events 
from {}
iam_role {}
format as json {}
region 'us-west-2';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs 
from {}
iam_role {}
format as json 'auto'
region 'us-west-2';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
Insert Into songplay
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT TIMESTAMP 'epoch' + evt.ts/1000 *INTERVAL '1 second' as start_time,
        evt.user_id,
        evt.level,
        sng.song_id,
        sng.artist_id,
        evt.session_id,
        evt.location,
        evt.user_agent
    FROM staging_events evt
    JOIN staging_songs sng
        ON evt.artist = sng.artist_name
    WHERE TRIM(evt.page) = 'NextSong'
""")

user_table_insert = ("""
Insert Into users
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT evt.user_id,
            evt.firstname as first_name,
            evt.lastname as last_name,
            evt.gender,
            evt.level
        from staging_events evt
    WHERE TRIM(evt.page) = 'NextSong'       
""")

song_table_insert = ("""
Insert Into songs
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT sng.song_id,
            sng.title, 
            sng.artist_id, 
            sng.year, 
            sng.duration
        FROM staging_songs sng
""")

artist_table_insert = ("""
Insert Into artists
    (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
            artist_name as name,
            artist_location as location,
            CAST(artist_latitude AS FLOAT)  as latitude,
            CAST(artist_longitude AS FLOAT) as longitude
        FROM staging_songs  
""")

time_table_insert = ("""
INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time,
            EXTRACT (hour from start_time) as hour,
            EXTRACT (day from start_time) as day,
            EXTRACT (week from start_time) as week,
            EXTRACT (month from start_time) as month,
            EXTRACT (year from start_time) as year,
            EXTRACT (dow from start_time) as weekday 
        FROM staging_events
        WHERE TRIM(page) = 'NextSong' 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
