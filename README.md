# NYC Taxi Data Pipeline

This documentation explains the framework and data flow built to process New York City (NYC) taxi trip data. This project is designed as an end-to-end data pipeline running entirely within a Docker environment, covering data ingestion, storage, and transformation for analytical needs.

## Architecture and Technology

The system is built upon several key integrated technologies:

First, Apache Airflow serves as the main orchestrator. Its role is to ensure every step in the pipeline runs according to schedule and in the correct order. We use Airflow to trigger Python scripts and dbt commands.

Second, for initial data processing, we utilize a combination of Python scripts and Apache Spark. This allows the system to efficiently download large amounts of data from external sources and load it into the database.

Third, PostgreSQL is used as the primary Data Warehouse. Data is stored here in distinct layers (schemas) to maintain organization and separation of concerns, ranging from raw data to data ready for analysis.

Finally, we use dbt (data build tool) to handle data transformations within the database. dbt enables us to write business logic in SQL and automatically manage the creation of tables and views in the database.

## Data Flow

The data flow in this system is designed with a multi-layered approach using different database schemas:

1.  **Ingestion (Schema: raw)**
    The process begins with Python scripts that download taxi trip data and weather data. The fetched data is then stored in the `raw` schema in PostgreSQL. At this stage, the data still reflects its original form from the source, without significant changes. The resulting tables include `raw.taxi_trips` and `raw.weather_hourly`.

2.  **Staging (Schema: staging)**
    Once raw data is available, dbt takes over to perform initial cleaning. Models in this layer read data from the `raw` schema, adjust data types, standardize column names, and apply basic filters. The results are stored in the `staging` schema. The goal is to prepare clean and consistent data for subsequent processes.

3.  **Marts (Schema: marts)**
    The final layer is the data mart, stored in the `marts` schema. Here, data from staging is combined and aggregated to answer specific business questions. Examples include calculating daily revenue, analyzing popular pickup locations, or observing the impact of weather on trip volume. Tables in this layer are ready to be used directly by data analysts or visualization tools.

## Project Structure

The project folder is organized to separate the responsibilities of each component:

*   **dags/**: Contains Airflow pipeline definitions (.py files) that manage execution schedules.
*   **scripts/**: Storage for Python code used for data extraction and loading (EL) processes.
*   **dbt_nyc/**: The dbt project directory containing all SQL transformation logic, model configurations, and data tests.
*   **data/**: A local folder used as a temporary landing zone for downloaded files before loading into the database.
*   **docker-compose.yaml**: The main configuration file for running all services (Airflow, Postgres) in containers.

## How to Run

To run this pipeline, you simply need to use Docker Compose. The command `docker compose up -d` will start all necessary services. Once all containers are running, you can access the Airflow web interface at `localhost:8080` to trigger and monitor the pipeline execution. To view the data results, you can access the exposed PostgreSQL database on port 5432.
