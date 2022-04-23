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

# Track dataframe. Note when we import our tracks as nodes we do not need to include
# the "playlist_id" column as a property——that info is only extracted for convenience
# for creating the relationship dataframe
track_slices = [get_track_data(i) for i in range(0, 2000, 1000)]
# just change 2000 to 1000000 to retrieve all songs
tracks_raw = pd.concat(track_slices, ignore_index=True)

# Now we combine duplicates to form a dataframe which contains only unique tracks.
# For every track, its "playlist_id" column contains a list of all playlists it is
# contained in:
tracks = tracks_raw.groupby(["track_name", "artist_name"]).agg({"pos": "first",
                                                                "track_uri": "first",
                                                                "artist_uri": "first",
                                                                "album_uri": "first",
                                                                "duration_ms": "first",
                                                                "album_name": "first",
                                                                "playlist_id": list})
# "first" means the column is included but remains unchanged
tracks = tracks.reset_index().reset_index()
# reset once first to pop out the "track_name" and "artist_name" multiindex as columns,
# then again to use the new index sequence [0, 1,....] as our "song_id" identifier
tracks = tracks.rename(columns={"index": "song_id"})
print(tracks)

# Example of a few entries——we can see that "A Sky Full of Stars - Hardwell Remix" appears
# in 4 playlists
print(tracks.loc[1063:1067])

print(tracks["track_name"].value_counts())
print(tracks["artist_name"].value_counts())

# Playlist dataframe
playlist_slices = [get_playlist_data(i) for i in range(0, 2000, 1000)]
# just change 2000 to 1000000 to retrieve all data
playlists = pd.concat(playlist_slices, ignore_index=True)
playlists = playlists.rename(columns={"pid": "playlist_id"})
print(playlists)
print(playlists.info())

# Relationship dataframe

# Now, all that's left to do is extract the song_id and playlist info into a new
# "relationship" dataframe. For every song, take its song_id and iterate over its
# playlist_id to create as many rows as needed.

# E.g. for "A Sky Full of Stars - Hardwell Remix" we should create
# 4 rows of [1066, 150], [1066, 330], [1066, 1712], and [1066, 1810]
# this can be done easily using pd.explode()

relationships = tracks[["song_id", "playlist_id"]]
relationships = relationships.explode("playlist_id", ignore_index=True)
print(relationships)
print(relationships[relationships["song_id"] == 1066])
print(relationships.info())

# To import relationships into Neo4j, now we all have to do is use this dataframe——
# the song_id and playlist_id columns represent our links (e.g. song_id = 3 and
# playlist_id = 0 means the 4th song is in the 1st playlist)

# However, this is possibly not done. We may still want to do some cleaning to remove
# metadata for memory reasons——maybe playlist descriptions are unnecessary






# –––––Explanation to other ppl, no need to read this really––––– #

# Note our method of detecting duplicates used a combination of "track_name" and "artist_name".
# This is different from using "track_uri"! That catches less duplicates because track_uri
# is a unique ID that Spotify assigns to every track, but often times the  same song is
# released in different areas, with slightly different lengths and versions. Our code will
# catch this whereas track_uri will incorrectly (for our purposes) interpret the tracks as
# unique. With that being said, it is still not entirely foolproof——we did not incorporate
# regular expression matching to check for similarly named song versions, etc.

