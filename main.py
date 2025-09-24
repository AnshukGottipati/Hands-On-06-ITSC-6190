from pyspark.sql import SparkSession

OUT_FORMAT = "csv"

def save(df, path):
    w = df.write.mode("overwrite")
    if OUT_FORMAT == "csv":
        w = w.option("header", "true")
    w.format(OUT_FORMAT).save(path)

spark = (SparkSession.builder
         .appName("MusicAnalysis")
         .getOrCreate())

# Load datasets
logs = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv("./listening_logs.csv"))

songs = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("./songs_metadata.csv"))

logs.createOrReplaceTempView("logs")
songs.createOrReplaceTempView("songs")


# Task 1: User Favorite Genres
favorite_genres = spark.sql("""
WITH counts AS (
  SELECT l.user_id, s.genre, COUNT(*) AS plays
  FROM logs l
  JOIN songs s USING (song_id)
  GROUP BY l.user_id, s.genre
),
max_counts AS (
  SELECT user_id, MAX(plays) AS max_plays
  FROM counts
  GROUP BY user_id
)
SELECT c.user_id, c.genre AS favorite_genre, c.plays
FROM counts c
JOIN max_counts m
  ON c.user_id = m.user_id AND c.plays = m.max_plays
ORDER BY c.user_id, favorite_genre
""")
favorite_genres.show(truncate=False)

# Task 2: Average Listen Time
avg_listen_time_per_song = spark.sql("""
SELECT l.song_id,
       s.title,
       s.artist,
       s.genre,
       ROUND(AVG(CAST(l.duration_sec AS DOUBLE)), 2) AS avg_listen_seconds
FROM logs l
LEFT JOIN songs s USING (song_id)
GROUP BY l.song_id, s.title, s.artist, s.genre
ORDER BY avg_listen_seconds DESC
""")
avg_listen_time_per_song.show(truncate=False)

# Task 3: Genre Loyalty Scores
genre_loyalty = spark.sql("""
WITH counts AS (
  SELECT l.user_id, s.genre, COUNT(*) AS plays
  FROM logs l
  JOIN songs s USING (song_id)
  GROUP BY l.user_id, s.genre
),
totals AS (
  SELECT user_id, SUM(plays) AS total_plays
  FROM counts
  GROUP BY user_id
),
max_counts AS (
  SELECT user_id, MAX(plays) AS max_plays
  FROM counts
  GROUP BY user_id
),
top AS (
  SELECT c.user_id, c.genre, c.plays
  FROM counts c
  JOIN max_counts m
    ON c.user_id = m.user_id AND c.plays = m.max_plays
)
SELECT t.user_id,
       t.genre AS top_genre,
       t.plays AS top_genre_plays,
       tot.total_plays,
       ROUND(t.plays / tot.total_plays, 3) AS loyalty_score
FROM top t
JOIN totals tot USING (user_id)
ORDER BY t.user_id, top_genre
""")
genre_loyalty.show(truncate=False)


# Task 4: Identify users who listen between 12 AM and 5 AM
night_users = spark.sql("""
SELECT DISTINCT user_id
FROM (
  SELECT user_id,
         HOUR(to_timestamp(`timestamp`)) AS hr
  FROM logs
)
WHERE hr BETWEEN 0 AND 5
ORDER BY user_id
""")
night_users.show(truncate=False)

save(favorite_genres,          "output/user_favorite_genres")
save(avg_listen_time_per_song, "output/avg_listen_time_per_song")
save(genre_loyalty,            "output/genre_loyalty_scores")
save(night_users,              "output/night_users")
