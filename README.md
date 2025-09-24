# Music Streaming Analysis Using Spark Structured APIs

## Overview
Using **Spark SQL** to run queries on two CSV datasets—user listening logs and song metadata—to compute user and content insights (e.g., average listening time, genre loyalty, night-owl behavior, and favorite genres).
## Dataset Description
Two CSV files power the analysis:

1. **`listening_logs.csv`**  
   - `user_id` – Unique ID of the user  
   - `song_id` – Unique ID of the song  
   - `timestamp` – When the song was played (e.g., `2025-03-23 14:05:00`)  
   - `duration_sec` – Duration in seconds for which the song was played

2. **`songs_metadata.csv`**  
   - `song_id` – Unique ID of the song  
   - `title` – Title of the song  
   - `artist` – Name of the artist  
   - `genre` – Genre (e.g., Pop, Rock, Jazz)  
   - `mood` – Mood (e.g., Happy, Sad, Energetic, Chill)

> Join key: `song_id`

## Repository Structure
```
handson/
├── output/
│ ├── avg_list_time/
│ │ └── result.csv
│ ├── genre_loyatlety score/ # (kept as-is; note the spelling)
│ │ └── result.csv
│ ├── night_users/
│ │ └── result.csv
│ └── user_fav_genre/
│ └── result.csv
├── .gitignore
├── datagen.py
├── listening_logs.csv
├── main.py
├── README.md
├── Requirements
└── songs_metadata.csv
```
## Output Directory Structure
```
├── output/
│ ├── avg_list_time/
│ │ └── result.csv
│ ├── genre_loyatlety score/ # (kept as-is; note the spelling)
│ │ └── result.csv
│ ├── night_users/
│ │ └── result.csv
│ └── user_fav_genre/
│ └── result.csv
```
## Tasks and Outputs
1. Find each user’s favourite genre: Identify the most listened-to genre for each user by
counting how many times they played songs in each genre.
2. Calculate the average listen time per song: Compute the average duration (in
seconds) for each song based on user play history.
3. Compute the genre loyalty score for each user: For each user, calculate the
proportion of their plays that belong to their most-listened genre. Output users with a
loyalty score above 0.8.
4. Identify users who listen to music between 12 AM and 5 AM: Extract users who
frequently listen to music between 12 AM and 5 AM based on their listening timestamps.
## Execution Instructions
```
python -m venv venv
pip install pyspark
python3 input_generator.py
#Can read the terminal to see what the top 20 lines of each query are
spark-submit main.py
#Now check the output file

```
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
