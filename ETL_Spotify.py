import requests
from datetime import datetime, timedelta
import pandas as pd
import json
import sqlalchemy
import sqlite3

# constants
# care token might expire after several minutes (will do an error 401)
TOKEN = "BQBO2uNynfzFsnFVfyjg1fK_-8EflU929xv6Jm1l8dSNGgNR1YE81blWMiujoQd9mZU_kxf-WZknbDQN1trjedkCx6I06HfT_Ei6tC4GzE-BWhLgXkrotvZIrSPT437X0RcgtzIRulmo19i_o9CVL9tTTaTNlKEGiNJlyZmHQiEgQpBDNovBjQ"
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

def check_if_valid_data(df: pd.DataFrame) -> bool:
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

if __name__ == "__main__":

    # EXTRACT ---------------------------------------------------------------------------------------------------------------------------------------------

    # time
    yesterday_datetime = datetime.now() - timedelta(days=1)
    yesterday_timestamp_milliseconds = int(yesterday_datetime.timestamp()) * 1000
    yesterday_timestamp_milliseconds = int(yesterday_datetime.timestamp()) * 1000

    # headers of our request
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }

    # making our request
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_timestamp_milliseconds), headers = headers)
    data = r.json()
    #r.status_code

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
    df_songs = pd.DataFrame(dictionary)

    # TRANSFORM -------------------------------------------------------------------------------------------------------------------------------------------
    
    if check_if_valid_data(df_songs):
        print("Data valid proceed to Load stage")

    # LOAD ------------------------------------------------------------------------------------------------------------------------------------------------

    engine= sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect('my_played_tracks.sqlite')
    cursor = connection.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_names VARCHAR(200),
        artist_names VARCHAR(200),
        played_at VARCHAR(200),
        timestamps VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        df_songs.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    connection.close()
    print("Close database successfully")

print(df_songs)