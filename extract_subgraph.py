from preprocessing import tracks, playlists, relationships
from pagerank import top50
import pandas as pd

playlists50 = playlists.loc[top50["pid"]]
print(playlists50.head())

track_indices = []
for i in range(len(tracks)):
    for pid in top50["pid"]:
        if pid in tracks.iloc[i]["playlist_id"]:
            track_indices.append(i)

tracks50 = tracks.loc[track_indices]
print(tracks50.head())

relationships50 = tracks50[["song_id", "playlist_id"]]
relationships50 = relationships50.explode("playlist_id", ignore_index=True)
print(relationships50.head())

playlists50.to_pickle("/Users/Terru/Desktop/UCLA/DataRes/spotify-project/dataframes/playlists50.pkl")
tracks50.to_pickle("/Users/Terru/Desktop/UCLA/DataRes/spotify-project/dataframes/tracks50.pkl")
relationships50.to_pickle("/Users/Terru/Desktop/UCLA/DataRes/spotify-project/dataframes/relationships50.pkl")