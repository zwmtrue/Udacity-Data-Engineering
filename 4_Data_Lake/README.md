In this project, we use Spark and data lakes to build an ETL pipeline for a data lake hosted on S3 for music streaming startup Sparkify.
  
The datasets include the Song Dataset and Log Dataset.  

Output tables include  

- Fact Table  
1. songplays - records in log data associated with song plays i.e. records with page NextSong  
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension Tables  
1. users - users in the app  
user_id, first_name, last_name, gender, level
  
2. songs - songs in music database  
song_id, title, artist_id, year, duration  

3. artists - artists in music database  
artist_id, name, location, lattitude, longitude  
  
4. time - timestamps of records in songplays broken down into specific units  
start_time, hour, day, week, month, year, weekday

To start the ETL process, fill in the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as is in the df.cfg file, then run python etl.py .   
Note: there is a known bug [https://github.com/aws/aws-cli/issues/602] with aws that the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY cannnot have any special characters in it.
Keep regenerating until you have a good pair.

dev.ipynb is a notebook for prototyping.  

