import pandas as pd
import json

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
print(tracks)

# ----------------------
# Creates our tracks dataframe which contains only unique tracks, with all playlists a given
# track is in displayed as a list
tracks_unique = tracks.groupby(["track_name", "artist_name"]).agg({"pos": "first", "track_uri": "first",
                                                                   "artist_uri": "first", "album_uri": "first",
                                                                   "duration_ms": "first", "album_name": "first",
                                                                   "playlist_id": list})
# "first" means the column is included but left unchanged
tracks_unique = tracks_unique.reset_index().reset_index()
# reset once first to pop out the "track_name" and "artist_name" multiindex as columns,
# then again to use the new index sequence [0, 1,....] as our "song_id" identifier
tracks_unique = tracks_unique.rename(columns={"index": "song_id"})
print(tracks_unique)

# Example of a few entries——we can see that "A Sky Full of Stars - Hardwell Remix" appears in
# 4 playlists
print(tracks_unique.loc[1063:1067])

print(tracks_unique.info())
print(tracks_unique["track_name"].value_counts())
print(tracks_unique["artist_name"].value_counts())

#!#
# READ THIS
#!#

# LEFT TO-DO: Extract the playlist info into a new "relationship" dataframe
# For every song, take its song_id and iterate over its playlist_id to create
# as many rows as needed. E.g. for "A Sky Full of Stars - Hardwell Remix" we should create
# 4 rows of [1066, 150], [1066, 330], [1066, 1712], and [1066, 1810]


# –––––Explanation to other ppl, no need to read this really––––– #

# Note our method of detecting duplicates used a combination of "track_name" and "artist_name".
# This is different from using "track_uri"! That catches less duplicates because track_uri
# is a unique ID that Spotify assigns to every track, but often times the  same song is
# released in different areas, with slightly different lengths and versions. Our code will
# catch this whereas track_uri will incorrectly (for our purposes) interpret the tracks as
# unique. With that being said, it is still not entirely foolproof——we did not incorporate
# regular expression matching to check for similarly named song versions, etc.
