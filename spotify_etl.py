import requests
import pandas as pd
import json
import sqlalchemy
import sqlite3
import spotipy
from spotify_secrets import *
from datetime import datetime, timedelta


def get_spotify_token():
    # function to retrieve a token in order to request Spotify's API

    # Storing a dummy uri and the data's scope
    redirect_uri = "http://localhost:8888/callback"
    scope = "user-read-recently-played"

    # Spotify's authentification, gives our token if successful
    spotify_token = spotipy.util.prompt_for_user_token(
        username, scope, client_id=client_id,
         client_secret=client_secret, redirect_uri=redirect_uri
    )

    return spotify_token


def extract(spotify_token):
    # function to retrieve our data from Spotify's API

    # time
    yesterday_datetime = datetime.now() - timedelta(days=1)
    yesterday_timestamp_milliseconds = int(yesterday_datetime.timestamp()) * 1000

    # headers of our request
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=spotify_token)
    }

    # making our request
    r = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_timestamp_milliseconds),
        headers = headers
    )
    data = r.json()

    # creating lists, we'll fill them with the data from the json
    artist_names = []
    song_names = []
    played_at = []
    timestamps = []

    # loop to append our data
    for songs in data["items"]:
        artist_names.append(songs["track"]["album"]["artists"][0]["name"])
        song_names.append(songs["track"]["name"])
        played_at.append(songs["played_at"])
        timestamps.append(songs["played_at"][0:10])

    # storing the data into a dictionnary
    dictionary = {
        "artist_names":artist_names,
        "song_names":song_names,
        "played_at":played_at,
        "timestamps":timestamps
    }

    # converting my dictionnary into a dataframe
    df = pd.DataFrame(dictionary)
    return df


def transform(df: pd.DataFrame) -> bool:
    # function to validate our data

    # check if dataframe is empty 
    if df.empty:
        print("No songs downloaded, finishing execution")
        return False

    # primary key check
    if pd.Series(df["played_at"]).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    # check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # check that all timestamps are from yesterday
    yesterday_datetime = datetime.now() - timedelta(days=1)
    yesterday_datetime = yesterday_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamps"].tolist()
    for timestamp in timestamps:
        if datetime.strptime(timestamp, "%Y-%m-%d") < yesterday_datetime:
            raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    return True


def load(df: pd.DataFrame):
    # function to load our data into our SQLite database

    # store our database URL
    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

    # create our engine, connection and cursor to execute our SQL query
    engine= sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect('my_played_tracks.sqlite')
    cursor = connection.cursor()

    # our SQL query to create the table if it doesn't exist
    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_names VARCHAR(200),
        artist_names VARCHAR(200),
        played_at VARCHAR(200),
        timestamps VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    # execute our query
    cursor.execute(sql_query)
    print("Opened database successfully")

    # write records stored in our dataframe into our SQLite database,
    # if data already exists, then abort writing
    try:
        df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    # Count the number of rows inserted
    n_rows = len(df.index)
    print(f"{n_rows} rows inserted")

    # close our database connection
    connection.close()
    print("Close database successfully")


def etl():
    # ETL function

    # Retrieve our spotify token
    spotify_token = get_spotify_token()

    # Extract
    df_songs = extract(spotify_token)

    # Transform
    if transform(df_songs):
        print("Data valid proceed to Load stage")

    # Load
    load(df_songs)


if __name__ == "__main__":
    # execute our ETL function
    etl()