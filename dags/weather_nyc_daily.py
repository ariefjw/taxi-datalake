from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Constants
DAG_ID = 'weather_nyc_daily'
START_DATE = pendulum.datetime(2023, 1, 1, tz="Asia/Jakarta")
SCHEDULE_INTERVAL = '0 0 * * *'

# Paths
DATA_SCRIPT_PATH = '/opt/airflow/scripts/weather_nyc_daily/extract_load.py'
DBT_DIR = '/opt/airflow/dbt_nyc'

default_args = {
    'owner': 'arief',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    DAG_ID,
    default_args=default_args,
    start_date=START_DATE,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=3,
    doc_md=__doc__
) as dag:
    
    # Simulate 2023 execution date
    simulated_date = '2023{{ ds[4:] }}'


    # Wait for the taxi trips DAG to complete, simulating a cross-DAG dependency
    wait_for_taxi_trips = ExternalTaskSensor(
        task_id='wait_for_taxi_trips_daily',
        external_dag_id='transport_taxi_trips_daily',
        external_task_id='end',
        timeout=3600,
        poke_interval=60,
        mode='reschedule'
    )
    
    # Fetches from API, saves Parquet, loads to Postgres
    ingest_weather = BashOperator(
        task_id='ingest_weather_daily',
        bash_command=f'python {DATA_SCRIPT_PATH} --date {simulated_date}'
    )
    
    # Cleans and standardizes raw weather data
    stg_weather = BashOperator(
        task_id='stg_weather_daily',
        bash_command=f'cd {DBT_DIR} && dbt run --select stg_weather --profiles-dir .'
    )
    
    # Joins weather with taxi trips
    fct_weather = BashOperator(
        task_id='fct_weather_impact_daily',
        bash_command=f'cd {DBT_DIR} && dbt run --select fct_weather_impact --profiles-dir .'
    )

    wait_for_taxi_trips >> ingest_weather
    
    ingest_weather >> stg_weather >> fct_weather
    
   
