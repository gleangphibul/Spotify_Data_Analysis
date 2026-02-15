# Data cleaning
WITH fixed_col_names AS (
SELECT 
Track AS track,
`Album Name` AS album_name,
Artist AS artist,
`Release Date` AS release_date,
ISRC AS isrc,
`All Time Rank` AS all_time_rank,
`Track Score` AS track_score,
`Spotify Streams` AS spotify_streams,
`Spotify Playlist Count` AS spotify_playlist_count,
`Spotify Playlist Reach` AS spotify_playlist_reach,
`Spotify Popularity` AS spotify_popularity,
`Youtube Views` AS youtube_views,
`Youtube Likes` AS youtube_likes,
`TikTok Posts` AS tiktok_posts,
`TikTok Likes` AS tiktok_likes,
`TikTok Views` AS tiktok_views,
`Youtube Playlist Reach` AS youtube_playlist_reach,
`Apple Music Playlist Count` AS apple_music_playlist_count,
`AirPlay Spins` AS airplay_spins,
`SiriusXM Spins` AS siriusxm_spins,
`Deezer Playlist Count` AS deezer_playlist_count,
`Deezer Playlist Reach` AS deezer_playlist_reach,
`Amazon Playlist Count` AS amazon_playlist_count,
`Pandora Streams` AS pandora_streams,
`Pandora Track Stations` AS pandora_track_stations,
`Soundcloud Streams` AS soundcloud_streams,
`Shazam Counts` AS shazam_counts,
`TIDAL Popularity` AS tidal_popularity,
`Explicit Track` AS explicit_track
FROM spotify_2024_fixed
),
fixed_date AS (
SELECT
*,
STR_TO_DATE(release_date, '%m/%d/%Y') AS release_date_cleaned
FROM fixed_col_names
),
cte AS (
SELECT 
*,
EXTRACT(YEAR FROM release_date_cleaned) AS release_year,
CASE 
  WHEN spotify_streams REGEXP '^[0-9,]+$' 
  THEN CAST(REPLACE(spotify_streams, ',', '') AS UNSIGNED INTEGER)
  ELSE NULL 
END AS spotify_streams_cleaned,
CASE 
	WHEN spotify_playlist_count REGEXP '^[0-9,]+$'
    THEN CAST(REPLACE(spotify_playlist_count, ',', '') AS UNSIGNED INTEGER)
    ELSE NULL
END AS spotify_playlist_count_cleaned,
CASE 
	WHEN spotify_playlist_reach REGEXP '^[0-9,]+$'
    THEN CAST(REPLACE(spotify_playlist_reach, ',', '') AS UNSIGNED INTEGER)
    ELSE NULL
END AS spotify_playlist_reach_cleaned
FROM fixed_date
),
# Ariana Grande
ag_1 AS (
SELECT
release_year, 
spotify_streams_cleaned
FROM cte
WHERE artist = 'Ariana Grande'
),
# Artist stream share
stream_share1 AS (
SELECT
artist,
spotify_streams_cleaned,
SUM(spotify_streams_cleaned) OVER () AS all_streams
FROM cte
),
stream_share2 AS (
SELECT
artist,
SUM(spotify_streams_cleaned) AS total_streams,
MAX(all_streams) AS all_streams
FROM stream_share1
GROUP BY artist
),
ranked_data AS (
SELECT
*,
ROW_NUMBER() OVER(ORDER BY total_streams DESC) AS rank_num
FROM stream_share2
)

# Top 10 songs spotify
/*
SELECT 
track,
artist,
release_date_cleaned,
spotify_streams_cleaned
FROM cte
ORDER BY spotify_streams_cleaned DESC
LIMIT 10;
*/


# Top 10 songs (no explicit)
/*
SELECT 
track,
artist,
release_date_cleaned,
spotify_streams_cleaned
FROM cte
WHERE explicit_track = 0
ORDER BY spotify_streams_cleaned DESC
LIMIT 10;
*/

# Top 10 Ariana Grande Songs
/*
SELECT
track,
artist,
release_date_cleaned,
spotify_streams_cleaned,
spotify_playlist_count, 
all_time_rank,
apple_music_playlist_count
FROM cte
WHERE artist = 'Ariana Grande'
ORDER BY spotify_streams_cleaned DESC
LIMIT 10;
*/

# Ariana Grande streams by year
SELECT
release_year, 
AVG(spotify_streams_cleaned) AS average_streams
FROM ag_1
GROUP BY release_year
ORDER BY release_year;

# Top 10 Artists based on spotify streams
/*
SELECT
artist,
SUM(spotify_streams_cleaned) AS total_streams
FROM cte
GROUP BY artist
ORDER BY total_streams DESC
LIMIT 10;
*/

# Top 10 Artist's stream share
/*
SELECT 
artist,
total_streams,
(total_streams / all_streams) * 100 AS stream_share
FROM ranked_data
WHERE rank_num <= 10

UNION ALL

SELECT
'Others' AS artist,
SUM(total_streams) AS total_streams,
(SUM(total_streams) / MAX(all_streams)) * 100 AS stream_share
FROM ranked_data
WHERE rank_num > 10

ORDER by stream_share DESC;
*/







#DESCRIBE spotify_2024_fixed
#EXTRACT YEAR FROM 'Release Date' OVER() AS release_year