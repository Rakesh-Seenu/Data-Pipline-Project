from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from data_fetch import data_ingestion_to_gcs, bucket_client, fetch_genre_data
from bucket_bigq import bucket_to_bigq, bigq_client
import logging

with DAG(dag_id="DE-2", start_date=datetime(2024, 6, 7), schedule_interval='0 0 1 */6 *') as dag:
    t1 = PythonOperator(
        task_id="data_to_bucket",
        python_callable=data_ingestion_to_gcs,
        dag=dag,
        op_args=[bucket_client]
    )

    t2 = PythonOperator(
        task_id="genre_data_to_bucket",
        python_callable=fetch_genre_data,
        dag=dag,
        op_args=[bucket_client]
    )

    t3 = PythonOperator(
        task_id="bucket_to_bigq",
        python_callable=bucket_to_bigq,
        dag=dag,
        op_args=[bigq_client]
    )

    # Set task dependencies
    t1 >> t2 >> t3