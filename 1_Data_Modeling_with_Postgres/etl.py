import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This procedure process a song file specified by filepath and extracts song and 
    artist information and stores in respective tables. 

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the song file
    RETURNS:
    * None
    """
    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ='series')])

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This procedure process a log file specified by filepath and extracts user and 
    songplay information and stores in respective tables. 

    INPUTS:
    * cur the cursor variable
    * filepath the file path to the log file
    RETURNS:
    * None
    """
    # open log file
    df_log = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_log = df_log[df_log['page']=='NextSong']

    # convert timestamp column to datetime
    df_log['ts'] = pd.to_datetime(df_log['ts'], unit='ms')
    ts = df_log['ts']
    
    # insert time data records
    time_data = [ts, ts.dt.hour, ts.dt.day, ts.dt.week, ts.dt.month, ts.dt.year, ts.dt.weekday]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.concat(time_data, axis=1)
    time_df.columns=column_labels
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


    # load user table
    user_df = df_log[['userId', 'firstName','lastName','gender','level']] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_log.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select,  (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function process all json files under filepath and calls the function func for 
    further processing.
    
    INPUTS: 
    * cur the cursor variable
    * conn connection object 
    * filepath the file path to directroy where all files are located.
    * func function to process these files.
    RETURNS:
    * None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()