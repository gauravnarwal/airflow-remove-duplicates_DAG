from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor
import requests
import os
import pandas as pd

INPUT_DIR="/opt/airflow/data/input"
PROCESSED_DIR="/opt/airflow/data/processed"
FILENAME= "sample.csv"

def remove_duplicates_and_move():
    input_path = os.path.join(INPUT_DIR, FILENAME)
    processed_path = os.path.join(PROCESSED_DIR, FILENAME)

    # 1. Read the file
    df = pd.read_csv(input_path, header=None, names=["fruit"])

    # 2. Normalize data
    df["fruit"] = df["fruit"].str.strip().str.lower()

    # 3. Remove duplicates
    df_cleaned = df.drop_duplicates()

    # 4. Write output
    df_cleaned.to_csv(processed_path, index=False, header=False)
    os.remove(input_path)

with DAG(
    dag_id="remove_dups",
    start_date=datetime(2025,1,1),
    schedule_interval='@daily',
    catchup=False,  
)as dag:
    task1=PythonOperator(
        task_id="remove_duplicateand_move",
        python_callable=remove_duplicates_and_move,
    )
    task1
