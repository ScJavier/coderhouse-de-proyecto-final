from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from tasks_etl_exchange_rates import get_data, process_data, load_data_to_db, send_confirmation_email

default_args={
    'owner': 'JavierSantibáñez',
    'retries':0,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    default_args=default_args,
    dag_id='etl_exchange_rates',
    description= 'ETL para extracción de tasas de cambio - proyecto final',
    start_date=datetime(2024,4,1,2),
    schedule_interval='@daily',
    catchup=False
    ) as dag:

    prework = EmptyOperator(task_id='prework_task')
    
    extract_data = PythonOperator(
        task_id='extract_data',
        provide_context=True,
        python_callable=get_data
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        provide_context=True,
        python_callable=process_data
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db
    )

    send_email = PythonOperator(
        task_id='send_email',
        provide_context=True,
        python_callable=send_confirmation_email
    )
    
    postwork = EmptyOperator(task_id='postwork_tast')
    
    prework >> extract_data >> transform_data >> load_data >> send_email >> postwork