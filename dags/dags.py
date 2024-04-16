from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from Airflow_ETL.T_Grammy import load_db, transform_db
from Airflow_ETL.T_Spotify import load_csv, transform_csv
from Airflow_ETL.Me_Lo_St import merge, load, store


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 13),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'dags_workshop2',
    default_args=default_args,
    description='Our first DAG with ETL process!!',
    schedule_interval='@daily',
) as dag:

    merge = PythonOperator(
        task_id = 'merge',
        python_callable = merge,
        provide_context = True,
    )

    load_csv = PythonOperator(
        task_id = 'load_csv',
        python_callable = load_csv,
        provide_context = True,
    )

    transform_csv = PythonOperator(
        task_id = 'transform_csv',
        python_callable = transform_csv,
        provide_context = True,
    )

    load_db = PythonOperator(
        task_id = 'load_db',
        python_callable = load_db,
        provide_context = True,
    )

    transform_db = PythonOperator(
        task_id = 'transform_db',
        python_callable = transform_db,
        provide_context = True,
    )

    store = PythonOperator(
        task_id='store',
        python_callable = store,
        provide_context = True,
    )

    load = PythonOperator(
        task_id ='load',
        python_callable = load,
        provide_context = True,
    )

    load_csv >> transform_csv >> merge
    load_db >> transform_db >> merge
    merge >> load >> store