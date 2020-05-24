This is project builds the etl pipeline for music streaming startup Sparkify. Raw data in json were extracted, processed and loaded to database in a star schema. The resulting database allows the analysis team to easily query data and understands what songs are users listening to.  

This project uses a star schema.  
The fact table is songplays, dimension tables are users, songs, artists, time, connected to facts table via user_id, song_id, artist_id and start_time respectively.

Detailed table structures:  
Fact table:
    songplays - records in log data associated with song plays i.e. records with page NextSong, this is PK
    songplay_id varchar, start_time timestamp, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id, location, user_agent
Dimension Tables:
    users - users in the app
    user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar
    songs - songs in music database
    song_id varchar, title varchar, artist_id varchar, year int, duration float
    artists - artists in music database
    artist_id varchar, name varchar, location varchar, latitude float, longitude float
    time - timestamps of records in songplays broken down into specific units
    start_time timestamp, hour int, day int, week int, month int, year int, weekday int

There are 3 python scripts and two notebooks included. 
    create_table.py drop and creates all the tables in this star schema.
    sql_queries.py stores are the sql queries to drop, create tables and insert data.
    etl.py is extract, process and load data into the database.
    test.ipynb provides a quick way to test the queries.
    etl.ipynb is a demo of the etl process.

Note than our database allows only one connection and it is necessary to restart the kernel or close the connection to run another notebook or python script.

To list the 100 most played songs run this query in test.ipynb:
%sql select  s.title, count(sp.user_id) from songplays sp inner join songs s on sp.song_id = s.song_id group by s.title order by count(sp.user_id) desc limit 100