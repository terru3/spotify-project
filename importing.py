import pandas as pd
import json

def get_data(i):
    data_slice = json.load(
        open(f'/Users/Terru/Desktop/UCLA/DataRes/spotify_million_playlist_dataset/data/mpd.slice.{i}-{i + 999}.json'))
    # change directory as needed
    df_slice = pd.json_normalize(data_slice["playlists"], record_path="tracks",
                           meta=['name', 'collaborative', 'pid', 'modified_at', 'num_tracks', 'num_albums',
                                 'num_followers', 'num_edits', 'duration_ms', 'num_artists', 'description'],
                           meta_prefix='playlist-', errors='ignore')
    return df_slice

df_slices = [get_data(i) for i in range(0, 2000, 1000)]
# just change 2000 to 1000000 to retrieve all data
df = pd.concat(df_slices, ignore_index=True)
print(df)

print(df.info())
print(df["track_name"].value_counts())
print(df["artist_name"].value_counts())

# Non-null descriptions of playlists
# print(df["description"].dropna())