import requests
from google.cloud import storage
import pandas as pd
from datetime import datetime
import io
import logging

# API details
BASE_URL = "https://api.themoviedb.org/3"

# GCS bucket details
BUCKET_NAME = "data_engg__project" 
FILE_NAME = "movie_data.csv"
GENRE_FILE_NAME = "genre_data.csv"
bucket_client = storage.Client()

# Function to fetch data from a given endpoint
def fetch_data(endpoint, params={}):
    url = f"{BASE_URL}/{endpoint}"
    headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJiZGZjZTAyYmJjNjNkYzhlYmE4NTUxNjViOTBkYzIzZCIsInN1YiI6IjY2NWI2NGU1NTQxYmE2MGVlMDQ3ZDdlMyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.1uHGsjCU5ackrT2L7_widNdH7do-ZMCsi6Gn_B2BVkY"}
    
    response = requests.get(url, params=params, headers=headers)
    return response.json()

# Function to fetch genre data from the API
def fetch_genre_data(client):
    logging.info("Fetching genre data")
    bucket = client.bucket(BUCKET_NAME)
    genre_blob = bucket.blob(GENRE_FILE_NAME)

    # Check if the genre data file exists in the bucket
    if genre_blob.exists():
        logging.info(f"Genre data already exists in the bucket {BUCKET_NAME} as {GENRE_FILE_NAME}")
        return

    # Fetch genre data from the API
    genres_response = fetch_data('genre/movie/list')
    genre_data = genres_response['genres']

    # Convert genre data to DataFrame and save as CSV
    genre_df = pd.DataFrame(genre_data)
    logging.info(genre_df)
    genre_csv_data = genre_df.to_csv(index=False)

    # Upload genre CSV to GCS
    genre_blob.upload_from_string(genre_csv_data, content_type='text/csv')
    logging.info(f"Genre data uploaded to GCS bucket {BUCKET_NAME} as {GENRE_FILE_NAME}")

# Limitation of the TMDB API
max_pages_to_retrieve = 501

# Function to get the data from the API and store it in GCS
def data_ingestion_to_gcs(client):
    logging.info("Starting data ingestion to GCS")
    bucket = client.bucket(BUCKET_NAME)
    if not bucket.exists():
        bucket = client.create_bucket(BUCKET_NAME)
        logging.info(f"Created bucket {BUCKET_NAME}")

    blob = bucket.blob(FILE_NAME)

    # Check if the file exists in the bucket
    if blob.exists():
        # Download the existing CSV file
        existing_data = blob.download_as_string().decode("utf-8")
        existing_df = pd.read_csv(io.StringIO(existing_data))
    else:
        existing_df = pd.DataFrame()

    # list to store all the movie data
    all_movies = []

    # Get the current year and calculate the previous year
    current_year = datetime.now().year
    previous_year = current_year - 1

    if datetime.now().month >=6:
        given_range = zip(range(7, 13, 3), range(9, 14, 3), [30, 30, 31])
    else:
        given_range = zip(range(1, 7, 3), range(3, 9, 3), [31, 30, 30])

    # Fetch data for the past 6 months of the previous year
    for month_start, month_end, month_day in given_range:
        for page_no in range(1, max_pages_to_retrieve):
            params = {
                'page': page_no,
                'include_adult': False,
                'primary_release_date.gte': f'{previous_year}-{month_start:02}-01',
                'primary_release_date.lte': f'{previous_year}-{month_end:02}-{month_day}'
            }
            response = fetch_data('discover/movie', params)
            all_movies.extend(response['results'])
            logging.info(f"Fetched page {page_no} of {response['total_pages']} for {previous_year}-{month_end}")
            if page_no == response['total_pages']:
                break

    # Convert data to DataFrame and append to existing DataFrame
    new_df = pd.DataFrame(all_movies)
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Save the combined DataFrame as CSV
    csv_data = combined_df.to_csv(index=False)

    # Upload CSV to GCS
    blob.upload_from_string(csv_data, content_type='text/csv')
    logging.info(f"Data uploaded to GCS bucket {BUCKET_NAME} as {FILE_NAME}")

if __name__ == "__main__":
    data_ingestion_to_gcs(bucket_client)
    fetch_genre_data(bucket_client)
