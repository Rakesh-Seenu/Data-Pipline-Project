from google.cloud import bigquery
from data_fetch import BUCKET_NAME, FILE_NAME, bucket_client, GENRE_FILE_NAME
import pandas as pd
import json
import io
import logging

# BigQuery details
PROJECT_ID = "datamanagementr"  
DATASET_ID = "movie_data_new"
TABLE_ID = "movie_details"
bigq_client = bigquery.Client(project=PROJECT_ID)

bucket = bucket_client.bucket(BUCKET_NAME)
genre_blob = bucket.blob(GENRE_FILE_NAME)

def preprocess_csv():
    logging.info("Preprocessing CSV data")
    client = bucket_client
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)
    csv_data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(csv_data), encoding='utf-8')

    # Handle missing quote characters in string columns
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        df[col] = df[col].str.replace(r'(^")|("$)', '', regex=True)
        df[col] = df[col].str.replace(r'""', '"', regex=True)

    # Load genre data from the CSV file
    genre_data = genre_blob.download_as_string().decode("utf-8")
    genre_df = pd.read_csv(io.StringIO(genre_data))

    # Create a dictionary mapping genre IDs to names
    genre_dict = {genre['id']: genre['name'] for _, genre in genre_df.iterrows()}

    # Replace genre IDs with a list of genre meanings
    df['genre_meaning'] = df['genre_ids'].apply(lambda ids: [genre_dict[id] for id in json.loads(ids)])
    # Convert genre_meaning to a string representation
    df['genre_meaning'] = df['genre_meaning'].apply(lambda x: ', '.join(x))

    return df

def bucket_to_bigq(client):
    logging.info("Starting data transfer from bucket to BigQuery")

    # Create the dataset if it doesn't exist
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
    except:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        logging.info(f"Created dataset {dataset.full_dataset_id}")

    # Create the table if it doesn't exist
    table_ref = dataset_ref.table(TABLE_ID)
    try:
        client.get_table(table_ref)
    except:
        table = bigquery.Table(table_ref)
        table = client.create_table(table)
        logging.info(f"Created table {table.full_table_id}")

    # Load data from GCS, preprocess it and then store it to BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True, quote_character='"', allow_quoted_newlines=True)

    df = preprocess_csv()

    logging.info(df)
    load_job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )

    logging.info(table_ref)
    load_job.result()  # Wait for the job to complete

    logging.info(f"Data loaded from GCS to BigQuery table {TABLE_ID}")

if __name__=="__main__":
    bucket_to_bigq(bigq_client)
