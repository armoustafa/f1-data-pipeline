from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from google.cloud import storage
import os

BUCKET_NAME = "vigilant-cider-447718-n5-f1-data-bucket"  # Update this if different
LOCAL_FOLDER = "data"

def upload_files_to_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    if not os.path.exists(LOCAL_FOLDER):
        print(f"❌ Folder '{LOCAL_FOLDER}' does not exist!")
        return

    files = os.listdir(LOCAL_FOLDER)
    if not files:
        print("❌ No files found in 'data/' folder")
        return

    for filename in files:
        if filename.endswith(".csv"):
            local_path = os.path.join(LOCAL_FOLDER, filename)
            blob = bucket.blob(f"raw/{filename}")
            blob.upload_from_filename(local_path, timeout=120)
            print(f"✅ Uploaded {filename} to GCS")
        else:
            print(f"⏩ Skipped non-CSV file: {filename}")

with DAG(
    dag_id="upload_f1_csvs_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    upload_task = PythonOperator(
        task_id="upload_csvs_to_gcs",
        python_callable=upload_files_to_gcs,
    )

if __name__ == "__main__":
    upload_files_to_gcs()