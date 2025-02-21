from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def clean_data():
    input_file = "/opt/airflow/dags/hotel_booking_data.csv"  # Path inside Docker
    output_file = "/opt/airflow/dags/cleaned_hotel_booking_data.csv"

    df = pd.read_csv(input_file)
    df_cleaned = df.dropna()
    df_cleaned.to_csv(output_file, index=False)

def cleaned_data_message():
    print("Hotel booking data has been successfully cleaned and saved.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 19),
    'retries': 1
}

with DAG(
    dag_id='hotel_booking_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data
    )

    message_task = PythonOperator(
        task_id='cleaned_data_message',
        python_callable=cleaned_data_message
    )

    clean_data_task >> message_task  # Set dependency
