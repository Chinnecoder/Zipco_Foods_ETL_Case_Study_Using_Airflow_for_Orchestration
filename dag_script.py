from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from Extraction import run_extraction
from Transformation import run_transformation
from Loading import run_loading


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 22),
    'email': ['okaforchinwendu@ymail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'zipco_foods_etl_pipeline',
    default_args= default_args,
    description= 'This represents the Zipco Foods Data Management Pipeline'
)

extraction = PythonOperator(
    task_id = 'extraction_layer',
    python_callable= run_extraction,
    dag=dag
)

transformation = PythonOperator(
    task_id = 'transformation_layer',       
    python_callable= run_transformation,
    dag=dag
)

loading = PythonOperator(
    task_id = 'loading_layer',
    python_callable= run_loading,
    dag=dag
)

# Set task dependencies (using left to right assignment)
extraction >> transformation >> loading