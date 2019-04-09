# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#songplay_id SERIAL PRIMARY KEY, \
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, \
                         start_time TIME NOT NULL, \
                         user_id INTEGER NOT NULL REFERENCES users (user_id), \
                         level VARCHAR, \
                         song_id VARCHAR REFERENCES songs (song_id), \
                         artist_id VARCHAR REFERENCES artists (artist_id), \
                         session_id VARCHAR , \
                         location VARCHAR, \
                         user_agent VARCHAR);""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, \
                    first_name VARCHAR, \
                    last_name VARCHAR, \
                    gender VARCHAR, \
                    level VARCHAR);""")


song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY, \
                         title VARCHAR, \
                         artist_id VARCHAR NOT NULL, \
                         year INTEGER, \
                         duration FLOAT);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY, \
                         name VARCHAR, \
                         location VARCHAR, \
                         lattitude VARCHAR, \
                         longitude VARCHAR);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIME, \
                         hour INTEGER, \
                         day INTEGER, \
                         week INTEGER, \
                         month INTEGER, \
                         year INTEGER, \
                         weekday INTEGER);""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
                    VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                    VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) \
                    VALUES(%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                    VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""")

# FIND SONGS

song_select = ("SELECT s.song_id AS song_id, a.artist_id AS artist_id \
                FROM songs s JOIN artists a ON s.artist_id = a.artist_id \
                WHERE s.title = (%s) OR a.name = (%s) OR s.duration = (%s)")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]