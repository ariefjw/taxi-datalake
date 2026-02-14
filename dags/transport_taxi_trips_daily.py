from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from scripts.transport_taxi_trips_daily.extract import download_data_for_date

# Constants
DAG_ID = 'transport_taxi_trips_daily'
START_DATE = pendulum.datetime(2023, 1, 1, tz="Asia/Jakarta")
SCHEDULE_INTERVAL = '0 0 * * *' 
TASK_ID_EXTRACT = 'ingestion_trips_daily'
TASK_ID_LOAD = 'source2landing_postgres'
TASK_ID_TRANSFORM_STAGING = 'landing_to_stg_trips_daily'
TASK_ID_TRANSFORM_TRIPS = 'stg2fac_trips_daily'
TASK_ID_TRANSFORM_HOTSPOTS = 'stg2fac_hotspots'
TASK_ID_TRANSFORM_FINANCIAL = 'stg2fac_financial'

# Paths
DBT_DIR = '/opt/airflow/dbt_nyc'
SCRIPT_PATH = '/opt/airflow/scripts/transport_taxi_trips_daily'

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
    

    # To simulate processing for the year 2023 regardless of the current year.
    simulated_date = '2023{{ ds[4:] }}'

    # Downloads Parquet data from the public NYC Taxi dataset to the local landing zone.
    source2landing = PythonOperator(
        task_id=TASK_ID_EXTRACT, 
        python_callable=download_data_for_date,
        op_kwargs={'execution_date': simulated_date}
    )
    
    # Ingests the downloaded Parquet file into the PostgreSQL 'raw_taxi_data' table using Spark.
    landing2stg = BashOperator(
        task_id=TASK_ID_LOAD,
        bash_command=f'python {SCRIPT_PATH}/load.py --date {simulated_date}'
    )

    # Builds the staging models which clean and prepare the raw data.
    transform_staging = BashOperator(
        task_id=TASK_ID_TRANSFORM_STAGING,
        bash_command=f'cd {DBT_DIR} && dbt run --select stg_nyc_tripdata --profiles-dir .'
    )

    # Transformation for daily trip summaries.
    stg2fac_trips = BashOperator(
        task_id=TASK_ID_TRANSFORM_TRIPS,
        bash_command=f'cd {DBT_DIR} && dbt run --select fct_taxi_trips_daily --profiles-dir .'
    )

    # Transformation for hotspot analysis.
    stg2fac_hotspots = BashOperator(
        task_id=TASK_ID_TRANSFORM_HOTSPOTS,
        bash_command=f'cd {DBT_DIR} && dbt run --select fct_taxi_hotspots --profiles-dir .'
    )

    # Transformation for financial efficiency metrics.
    stg2fac_financial = BashOperator(
        task_id=TASK_ID_TRANSFORM_FINANCIAL,
        bash_command=f'cd {DBT_DIR} && dbt run --select fct_financial_efficiency --profiles-dir .'
    )

    # Transform Metric Task (Spark) 
    calculate_metrics = BashOperator(
        task_id='calculate_trip_efficiency',
        bash_command=f'python {SCRIPT_PATH}/calculate_metrics.py --date {simulated_date}'
    )

    # End Task
    end = EmptyOperator(task_id='end')

    # Dependency Definitions
    fact_tasks = [stg2fac_trips, stg2fac_hotspots, stg2fac_financial]
    
    # data flow: source -> landing -> [staging (dbt) + metrics (spark)] -> facts -> end
    source2landing >> [landing2stg, calculate_metrics]
    landing2stg >> transform_staging >> fact_tasks >> end
    calculate_metrics >> end