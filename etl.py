import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    This function read a filepath, write it on a pandas dataframe,
    select the columns desired and insert into the correct table.
    Since there is only one song and artist in each file, the function
    simply get the first row of the dataframe to insert into the
    songs and artists table.
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_field_list = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_df = df[artist_field_list]
    artist_data = artist_df.values[0] 
    try:
        cur.execute(artist_table_insert,artist_data)
    except psycopg2.Error as e:
        print("Error: Insert artist")
        print(e)
        
    # insert song record
    song_field_list = ['song_id','title','artist_id','year','duration']
    song_df = df[song_field_list]
    song_data = song_df.values[0]
    try:
        cur.execute(song_table_insert,song_data)
    except psycopg2.Error as e:
        print("Error: Insert song")
        print(e)    
    

def process_log_file(cur, filepath):
    '''
    This function read a filepath, write it on a pandas dataframe,
    select the columns desired and insert into the correct table.
    Since there is multiple logs in each file, the script itterates 
    through each row of the dataframe to insert into the songplays
    and users table.
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong'] 

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # get time dataframe
    ts_list = t.values
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_matrix = [t,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday]
    time_df = pd.DataFrame(columns=column_labels)
    for i in range(len(column_labels)):
        time_df[column_labels[i]] = time_matrix[i]
        
    # insert time data records
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print('Error: Insert time')
            print(e)
            
    # load user table
    user_fields_list = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_fields_list]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycog2.Error as e:
            print('Error: Insert user')
            print(e)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print("Error: Select song_id and artist_id")
            print(e)
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Insert Songplays")
            print(e)


def process_data(cur, conn, filepath, func):
    '''
    This function itterates through all files in the
    data directory and run the respective ETL function
    to each of them.
    '''
    
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
    try: 
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get cursor to the Database")
        print(e)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()