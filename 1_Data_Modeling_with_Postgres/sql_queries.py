# DROP TABLES

songplay_table_drop = "Drop Table If Exists songplays;"
user_table_drop = "Drop Table if Exists users;"
song_table_drop = "Drop Table if Exists songs"
artist_table_drop = "Drop Table if Exists artists"
time_table_drop = "Drop Table if Exists time"

# CREATE TABLES

songplay_table_create = ("""
Create Table If Not Exists songplays(
    songplay_id  SERIAL PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id varchar NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int NOT NULL, 
    location varchar, 
    user_agent varchar
)
""")

# This table doesn't necessarily need any NOT NULL constraints, but the last reviewer has been demanding
user_table_create = ("""
Create Table If Not Exists users(
    user_id varchar PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL, 
    gender varchar, 
    level varchar
)
""")

song_table_create = ("""
Create Table If Not Exists songs(
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar NOT NULL, 
    year int, 
    duration float
)
""")

artist_table_create = ("""
Create Table If Not Exists artists(
    artist_id varchar PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude float, 
    longitude float
)
""")

# Time table doesn't need any NOT NULL constarint, and won't have null values as long as PK constarint is stasified.
time_table_create = ("""
Create Table If Not Exists time(
    start_time timestamp PRIMARY KEY,
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
)
""")

# INSERT RECORDS

songplay_table_insert = ("""
Insert Into songplays
    (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES
    ( %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
Insert Into users
    (user_id, first_name, last_name, gender, level)
    VALUES 
    (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
Insert Into songs 
    (song_id, title, artist_id, year, duration) 
    VALUES 
    (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
Insert Into artists
    (artist_id, name, location, latitude, longitude) 
    VALUES
    (%s, %s, %s, %s, %s)
    ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
Insert Into  time
    (start_time, hour, day, week, month, year, weekday) 
    VALUES
    (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS

song_select = ("""
select  s.song_id, a.artist_id 
    from songs s 
    inner join artists a 
        on s.artist_id=a.artist_id 
        and (s.title = %s) 
        and (a.name = %s) 
        and (s.duration = %s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]