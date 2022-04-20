import pandas as pd
import json

data = json.load(open('/Users/Terru/Desktop/UCLA/DataRes/spotify-project/spotify_million_playlist_dataset/data/mpd.slice.0-999.json'))
df = pd.json_normalize(data["playlists"], record_path="tracks", meta=['name', 'collaborative', 'pid', 'modified_at', 'num_tracks', 'num_albums', 'num_followers', 'num_edits', 'duration_ms', 'num_artists', 'description'], meta_prefix = 'playlist-', errors='ignore')
print(df)
print(df.info())
print(df["track_name"].value_counts())
print(df["artist_name"].value_counts())

# 20 non-null descriptions of playlists
# print(df["description"].dropna())