from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# List of F1 tables to load
f1_tables = [
    'circuits',
    'constructor_results',
    'constructor_standings', 
    'constructors',
    'driver_standings',
    'drivers',
    'lap_times',
    'pit_stops',
    'qualifying',
    'races',
    'results',
    'seasons',
    'status',
    'sprint_results'
]

with DAG(
    'gcs_to_bigquery_f1',
    default_args=default_args,
    description='Load F1 data from GCS to BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),  # Changed to days_ago(1) to avoid future dates
    catchup=False,
    tags=['f1', 'gcs', 'bq'],
) as dag:
    
    # Create a task for each table
    tasks = {}
    for table in f1_tables:
        tasks[table] = GCSToBigQueryOperator(
            task_id=f'load_{table}',
            bucket='{{ var.value.gcs_bucket }}',
            source_objects=[f'raw/{table}.csv'],
            destination_project_dataset_table=f'{{{{ var.value.bq_dataset }}}}.{table}',
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            autodetect=True,
            max_bad_records=10
        )
    
    # Set task dependencies (optional - if you want a specific order)
    # For example, load circuits first, then races, then results, etc.
    for i in range(len(f1_tables)-1):
        tasks[f1_tables[i]] >> tasks[f1_tables[i+1]]