from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'shashidhar.billakanti',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['shashidhar.billakanti05@gmail.com'],
    'depend_on_past': False,
}

with DAG(
    'gcs_to_bigquery',
    catchup=False,
    schedule_interval='@daily',
    default_args=default_args
) as dag:

    start=DummyOperator(task_id='start')
    end=DummyOperator(task_id='end')
    checking_bucket = GCSListObjectsOperator(
        task_id='checking_list_gcs',
        bucket='gcs_read_bucket',
        prefix='data/',
        gcp_conn_id='google_cloud_default'
    )
    
    load_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='gcs_read_bucket',
        source_objects=['data/sales.csv'],
        destination_project_dataset_table='learning-project-482611.learning_1.sales_1',
        skip_leading_rows=1,
        autodetect=True,  
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default'
    )
    
    start>>checking_bucket >> load_to_bq>>end
