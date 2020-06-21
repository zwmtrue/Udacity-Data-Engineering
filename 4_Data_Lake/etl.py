import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['aws']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['aws']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    this function get or create a spark session and return it.
    input: None
    output: spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    this function process song data, stores results into songs_table and artists_table
    input
        spark: spark session object
        input_data: path to s3 bucket where root directory of song-data is located
        out_put_data: path to write songs_table and artists_table
    """
    # get filepath to song data file
    # for testing, try a subset 
    song_data = input_data + "song-data/A/A/B/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_cols = ['song_id', 'title', 'artist_id', 'year','duration']
    songs_table = df.select(*songs_cols).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + "songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_cols = ['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']
    
    artists_table = df.select(*artists_cols).distinct().\
        withColumnRenamed("artist_name","name").\
        withColumnRenamed("artist_location","location").\
        withColumnRenamed("artist_lattitude","lattitude").\
        withColumnRenamed("artist_longitude","longitude")

    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    this function process log data, stores results into users_table, time_table and songplays_table
    input
        spark: spark session object
        input_data: path to s3 bucket where root directory of log-data is located
        out_put_data: path to write users_table, time_table and songplays_table
    """

    # get filepath to log data file
    log_data = input_data + "log-data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df["page"] == 'NextSong')

    # extract columns for users table    
    users_table = (
        df.select(
            col('userId').alias('user_id'),
            col('firstName').alias('first_name'),
            col('lastName').alias('last_name'),
            col('gender').alias('gender'),
            col('level').alias('level')
        ).distinct()
        )
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users.parquet", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp(ms/1000.0), T.TimestampType())
    df =df.withColumn("ts_timestamp",get_timestamp(df.ts)) 
       
    # extract columns to create time table
    time_table = (
    df.withColumn('hour', hour(col('ts_timestamp')))
        .withColumn('day', dayofmonth(col('ts_timestamp')))
        .withColumn('week', weekofyear(col('ts_timestamp')))
        .withColumn('month', month(col('ts_timestamp')))
        .withColumn('year', year(col('ts_timestamp')))
        .select(col("hour"),
            col("day"),
            col("week"),
            col("month"),
            col("year"),
            date_format('ts_timestamp', 'EEEE').alias('week_day')
               )
        )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + "time.parquet", mode="overwrite")

    # read in song data to use for songplays table
    # mergeSchema ensures artist_id and year columns will be in the final dataframe
    songs_df = spark.read.option("mergeSchema", "true").parquet(output_data + "songs.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
         df.join(songs_df, songs_df.title == df.song)
         .select(
             col('ts_timestamp').alias('start_time'),
             col('userId').alias('user_id'),
             col('level'),
             col('song_id'),
             col('artist_id'),
             col('sessionId').alias('session_id'),
             col('location'),
             col('userAgent').alias('user_agent'),
             month(col('ts_timestamp')).alias('month'),
             year(col('ts_timestamp')).alias("year")
         ).withColumn('songplay_id', F.monotonically_increasing_id()
            )
     )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + "songplays.parquet", mode="overwrite")


def main():
    """
    this script process uses pyspark to build an ETL pipeline for data hosted on S3 then upload to s3    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # replace output location with a s3 bucket curent user has write access to
    output_data = "s3a://de-zwmtrue/data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
