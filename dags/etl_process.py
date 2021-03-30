import datetime
import pandas as pd
import sqlalchemy
import spotipy
import sys
from spotipy.oauth2 import SpotifyOAuth
from settings import spotipy_client_id, spotipy_client_secret
from db import engine
from models import Song


def data_validation(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print('No songs downloaded. Finishing execution')
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null valued found")

    return True


def extraction():
    # Extract stage

    # Authentication into Spotify via OAuth
    cache_path = 'cache' if sys.platform in ("linux", "linux2") else None
    spotify = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=spotipy_client_id,
                                                        client_secret=spotipy_client_secret,
                                                        redirect_uri="http://localhost:8888",
                                                        scope="user-read-recently-played",
                                                        cache_path=cache_path
                                                        ))

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_time = int(yesterday.timestamp()) * 1000

    data = spotify.current_user_recently_played(after=yesterday_unix_time)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data['items']:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][:10])

    song_df = pd.DataFrame(
        {'song_name': song_names,
         'artist_name': artist_names,
         'played_at': played_at_list,
         'timestamp': timestamps
         })

    # Validate data
    if data_validation(song_df):
        print("Data valid, proceed to Load stage")

    return song_df


def save_to_database(song_df: pd.DataFrame):  # Load stage
    skipped = 0
    for row in range(len(song_df)):
        try:
            song_df.iloc[row:row + 1].to_sql(Song.__tablename__, engine, index=False, if_exists='append')
        except sqlalchemy.exc.IntegrityError:
            skipped += 1

    print(f"{skipped}/{len(song_df)} items were skipped (already in the database)") \
        if skipped > 0 \
        else print(f"Data loaded: {len(song_df)} items")


def run_spotify_etl():
    save_to_database(extraction())

