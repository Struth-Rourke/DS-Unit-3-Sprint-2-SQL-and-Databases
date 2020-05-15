import os
import sqlite3
import pandas as pd

##------------------------------------------------------------------------------
# 1. DB_FILEPATH, Connection, and Cursor to connect to SQLite3
##------------------------------------------------------------------------------
# DATABASE_FILEPATH
DATABASE_FILEPATH = os.path.join(os.path.dirname(__file__), "chinook.db")

# Connection to DATABASE_FILEPATH
lite_conn = sqlite3.connect(DATABASE_FILEPATH)

# Cursor to Connection
lite_curs = lite_conn.cursor()


##------------------------------------------------------------------------------
# 2. Queries 
##------------------------------------------------------------------------------
# Query 1: How many tracks per genre?
query1 = """
SELECT
	tracks.GenreId
	,genres.name
	,count(tracks.TrackId) as track_count 
FROM tracks
LEFT JOIN 
	genres on genres.GenreId = tracks.GenreId
GROUP BY 
	tracks.GenreId 
"""

# Query 1 ANSWER:
result = lite_curs.execute(query1).fetchall()
df = pd.DataFrame(result, columns = ['GenreId', 'Genre', 'Track Count'])
print('\n')
print(df.head())
print('\n')



# Query 2: How many tracks, per album, per genre; as well as average tracks per album?
query2 = """
SELECT
	tracks.GenreId
	,genres.name
	,count(distinct albums.AlbumId) as album_count
	,count(distinct tracks.TrackId) as track_count 
	,count(distinct tracks.TrackId) / count(distinct albums.AlbumId) as avg_tracks_per_album
FROM tracks
LEFT JOIN 
	genres on genres.GenreId = tracks.GenreId
	,albums on albums.AlbumId = tracks.AlbumId
GROUP BY 
	tracks.GenreId
"""

# Query 2 ANSWER:
result = lite_curs.execute(query2).fetchall()
df = pd.DataFrame(result, columns = ['GenreId', 'Genre', 'Album Count',
                                     'Track Count', 'Average Tracks per Album'])
print(df.head())
print('\n')



# Query 3: How many track names longer than 20 characters?
query3 = """
SELECT
	count(distinct tracks."Name") as tracks_longer_then_20
FROM tracks
WHERE
	LENGTH(tracks."Name") >= 20
"""

# Query 3 ANSWER:
result = lite_curs.execute(query3).fetchmany()
#breakpoint()
print("Tracks Longer than 20 Characters:", result[0][0])
print('\n')



# Query 4: Average Unit Price per album?
# query4 = """

# """

# # Query 3 ANSWER:
# result = curs.execute(query4).fetchall()
# print(result)
# print('\n')
