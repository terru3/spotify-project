import pandas as pd

from tqdm import tqdm
from neo4j import GraphDatabase

from preprocessing import tracks, playlists, relationships

print(tracks.head())
print(playlists.head())
print(relationships.head())

uri = "bolt://localhost:7687"

auth = ("neo4j","jatdatares")

driver = GraphDatabase.driver(uri = uri, auth = auth)
print(driver.verify_connectivity())

# Create uniqueness constraints
# query = "CREATE CONSTRAINT FOR (t:Track) REQUIRE t.song_id IS UNIQUE"
# info = driver.session().run(query)
# response = driver.session().run("CALL db.constraints").data()
# print(response)

# ^ ran already

# Import tracks
def create_tracks(tx, song_id, track_name, artist_name, pos, track_uri,
                 artist_uri, album_uri, duration_ms, album_name) -> None:

    query = """
            MERGE (t:Track {song_id: $song_id, track_name: $track_name,
            artist_name: $artist_name, pos: $pos, track_uri: $track_uri,
            artist_uri: $artist_uri, album_uri: $album_uri,
            duration_ms: $duration_ms, album_name: $album_name})
            """
    tx.run(query, song_id=song_id, track_name=track_name,
           artist_name=artist_name, pos=pos, track_uri=track_uri,
            artist_uri=artist_uri, album_uri=album_uri,
           duration_ms=duration_ms, album_name=album_name)

for i in tqdm(tracks.itertuples(), desc = "Deploying Track Nodes"):
    (_, song_id, track_name, artist_name, pos, track_uri,
    artist_uri, album_uri, duration_ms, album_name, __) = i

    driver.session().write_transaction(create_tracks, song_id,
                                       track_name, artist_name, pos, track_uri,
                                        artist_uri, album_uri, duration_ms,
                                       album_name)

print("Success", flush=True)



# Import playlists
def create_playlists(tx, name, collaborative, playlist_id, modified_at, num_tracks,
                 num_albums, num_followers, num_edits, duration_ms, num_artists,
                 description) -> None:

    query = """
            MERGE (p:Playlist {name: $name, collaborative: $collaborative,
            playlist_id: $playlist_id, modified_at: $modified_at,
            num_tracks: $num_tracks, num_albums: $num_albums,
            num_followers: $num_followers, num_edits: $num edits,
            duration_ms: $duration_ms, num_artists: $num_artists,
            description: $description})
            """
    tx.run(query, name=name, collaborative=collaborative, playlist_id=playlist_id,
           modified_at=modified_at, num_tracks=num_tracks, num_albums=num_albums,
           num_followers=num_followers, num_edits=num_edits, duration_ms=duration_ms,
           num_artists=num_artists, description=description)

for i in tqdm(playlists.itertuples(), desc = "Deploying Playlist Nodes"):
    (_, name, collaborative, playlist_id, modified_at, num_tracks, num_albums,
    num_followers, num_edits, duration_ms, num_artists, description) = i

    driver.session().write_transaction(create_playlists, name,
                                       collaborative, playlist_id, modified_at,
                                       num_tracks, num_albums, num_followers,
                                       num_edits, duration_ms, num_artists, description)

print("Success", flush=True)


# def create_relationships(tx, song_id, playlist_id) -> None:
#
#     query = """
#             MATCH (t:Track {song_id: $song_id})
#             MATCH (p:Playlist {playlist_id: $playlist_id})
#             MERGE (t)-[r:IN]->(p)
#             """
#     tx.run(query, song_id = song_id, playlist_id = playlist_id)
#
# for i in tqdm(relationships.itertuples(), desc = "Deploying Relationships"):
#     (_, song_id, playlist_id) = i
#     driver.session().write_transaction(create_relationships, song_id, playlist_id)
#
# print("Success", flush=True)
