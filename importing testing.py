import pandas as pd
import json

def get_playlist_data(i):
    data_slice = json.load(
        open(f'/Users/Terru/Desktop/UCLA/DataRes/spotify_million_playlist_dataset/data/mpd.slice.{i}-{i + 999}.json'))
    # change directory as needed
    df_slice = pd.json_normalize(data_slice["playlists"])
    df_slice = df_slice.drop(columns=["tracks"])
    return df_slice

def get_track_data(i):
    data_slice = json.load(
        open(f'/Users/Terru/Desktop/UCLA/DataRes/spotify_million_playlist_dataset/data/mpd.slice.{i}-{i + 999}.json'))
    # change directory as needed
    df_slice = pd.json_normalize(data_slice["playlists"], record_path="tracks", meta=["pid"])
    df_slice = df_slice.rename(columns={"pid": "playlist_id"})
    return df_slice

track_slices = [get_track_data(i) for i in range(0, 2000, 1000)]
# just change 2000 to 1000000 to retrieve all songs
tracks = pd.concat(track_slices, ignore_index=True)
tracks.index.names = ["song_id"]
tracks = tracks.reset_index()
print(tracks)

print(tracks.info())
print(tracks["track_name"].value_counts())
print(tracks["artist_name"].value_counts())

playlist_slices = [get_playlist_data(i) for i in range(0, 2000, 1000)]
# just change 2000 to 1000000 to retrieve all data
playlists = pd.concat(playlist_slices, ignore_index=True)
print(playlists)
print(playlists.info())

# To obtain and import relationships, now we all have to do is look at the tracks dataframe.
# The song_id and playlist_id columns represent our links
# (e.g. song_id = 3 and playlist_id = 0 means the 4th song is in the 1st playlist)

# However, this is not done. First of all we may want to do some cleaning to remove unnecessary
# metadata for memory reasons——maybe playlist descriptions are unnecessary. FURTHERMORE,
# this current setup treats duplicate songs as separate because we're using a unique song_id.
# Is this what we want? I'm not sure. We have to think about what kind of analysis we want
# to perform and how this might affect our ability to use GDS algorithms.