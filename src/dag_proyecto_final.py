from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args={
    'owner': 'JavierSantibanez',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def run_main():
    import main

with DAG(
    default_args=default_args,
    dag_id='test_1',
    description= 'Primera prueba para el proyecto final',
    start_date=datetime(2024,4,26,2),
    schedule_interval='@daily'
    ) as dag:
    task1= PythonOperator(
        task_id='run_main',
        python_callable= run_main,
    )

    task1