# Data-Pipline-Project

## Overview
This project involves building a data pipeline to fetch, process, and store data using Python scripts and Docker. The pipeline includes steps for fetching data from a source, processing the data, and storing it in a BigQuery database. Additionally, the data is fed into Tableau for visualization. The project utilizes Docker for containerization to ensure reproducibility and ease of deployment.

## Project Structure
- **bucket_bigq.py:** Script to interact with Google Cloud Storage and BigQuery.
- **data_fetch.py:** Script to fetch data from a specified source.
- **pipeline.py:** Script to define and execute the data pipeline.
- **docker-compose.yaml:** Docker Compose file to set up and run the services.

## Features
- **Data Fetching:** Fetches data from a specified source using `data_fetch.py`.
- **Data Processing:** Processes the fetched data to prepare it for storage.
- **Data Storage:** Stores the processed data into Google BigQuery using `bucket_bigq.py`.
- **Data Visualization:** Feeds the stored data into Tableau for creating interactive dashboards.
- **Containerization:** Uses Docker to containerize the scripts for consistent execution across different environments.

## Prerequisites
- **Docker:** Ensure Docker is installed on your machine.
- **Google Cloud SDK:** Ensure the Google Cloud SDK is installed and configured.
- **Python:** Python 3.x should be installed.
- **Tableau:** Tableau Desktop or Tableau Online for data visualization.

## Setup
1. **Clone the Repository:**
    ```sh
    git clone <repository-url>
    cd <repository-directory>
    ```

2. **Set Up Google Cloud Credentials:**
    Ensure your Google Cloud credentials are set up correctly. You can do this by setting the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your credentials JSON file.
    ```sh
    export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
    ```

3. **Build and Run Docker Containers:**
    Use Docker Compose to build and run the containers.
    ```sh
    docker-compose up --build
    ```

## Usage
- **Running the Pipeline:**
    The pipeline is set up to run automatically when the Docker container starts. You can check the logs to monitor the progress.
    ```sh
    docker-compose logs -f
    ```

- **Stopping the Pipeline:**
    To stop the running containers, use:
    ```sh
    docker-compose down
    ```

- **Visualizing Data in Tableau:**
    After the data is processed and stored in BigQuery, you can connect Tableau to your BigQuery instance to create interactive visualizations. 
    1. Open Tableau Desktop or Tableau Online.
    2. Connect to your Google BigQuery data source.
    3. Import the processed data and create your visualizations.

## Scripts Overview
- **bucket_bigq.py:**
    - Handles interactions with Google Cloud Storage and BigQuery.
    - Uploads processed data to a bucket and inserts it into BigQuery tables.

- **data_fetch.py:**
    - Fetches raw data from a specified source.
    - Can be configured to fetch data from various APIs or databases.

- **pipeline.py:**
    - Defines the steps of the data pipeline.
    - Coordinates the fetching, processing, and storing of data.

- **docker-compose.yaml:**
    - Defines the services for running the data pipeline in Docker containers.
    - Includes configurations for building images and setting up networks.

