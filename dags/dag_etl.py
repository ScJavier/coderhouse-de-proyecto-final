from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etl_exchange_rates import main

default_args={
    'owner': 'JavierSantibáñez',
    'retries':0,
    'retry_delay': timedelta(minutes=1)
}

def run_main(execution_date):
    main(execution_date)

with DAG(
    default_args=default_args,
    dag_id='etl_exchange_rates',
    description= 'ETL para extracción de tasas de cambio - proyecto final',
    start_date=datetime(2024,4,1,2),
    schedule_interval='@daily',
    catchup=False
    ) as dag:
    task1= PythonOperator(
        task_id='run_main',
        provide_context=True,
        python_callable= run_main
    )

    task1