import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function that reads a json song file, proccesses it and inserts it into a database into two tables.

    Parameters
    ----------
    cur
        Cursor of the created database.
    filepath : str
        Filepath of the song json file.
    """
    # open song file
    df = pd.read_json(path_or_buf=filepath, lines=True)

    # insert song record
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]
    song_data = df[song_cols].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_cols = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artist_data = df[artist_cols].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Function that reads a json log file, proccesses it and inserts it into a database into three tables.
    
    First function reads a json log file, filters it for the NextSong values in the page columns
    Timestamp column is converted and split into multiple columns, and inserted into a time table.
    User data is inserted into a user table in the database, and lastly songplays table is 
    created from a combination of song and log data json files based on the join query.

    Parameters
    ----------
    cur
        Cursor of the created database.
    filepath : str
        Filepath of the log json file.
    """
    # open log file
    df = pd.read_json(path_or_buf=filepath, lines=True)

    # filter by NextSong action
    filter = df["page"]=="NextSong"
    df = df[filter]

    # convert timestamp column to datetime
    df['start_time'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = ["hour", "day", "week", "month", "year", "weekday"]
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    for t in time_data:
        df[t] = getattr(df.start_time.dt, t) 
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_cols = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_df = df[user_cols]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ( row.start_time, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function gets all files matching a json extention from a directory,
    iterates over each file and processes it by a given function.
    
    Parameters
    ----------
    cur
        Cursor of the created database.
    conn
        Connection of the created database.
    filepath : str
        Filepath of the log json file.
    func
        function call in the argument
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
    """
    Main function tha establishes a connection to the database and creates a cursor.
    Proccesses the data for each log file and song file, and inserts it accordingly 
    to the database tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()