import logging
import os
import sys
import wget
import argparse
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date

# Constants & Configuration
from scripts.common.path_utils import PathConfig

JDBC_DRIVER_VERSION = "42.6.0"
JDBC_JAR_NAME = f"postgresql-{JDBC_DRIVER_VERSION}.jar"
JAR_DIR = "/opt/airflow/scripts/jars"
JAR_PATH = os.path.join(JAR_DIR, JDBC_JAR_NAME)
JDBC_URL = f"https://jdbc.postgresql.org/download/{JDBC_JAR_NAME}"

# Database Configuration
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST", "postgres")

if not all([POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB]):
    raise ValueError("Missing required database environment variables (POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB)")

POSTGRES_URL = f"jdbc:postgresql://{DB_HOST}:5432/{POSTGRES_DB}"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

from scripts.common.custom_logger import setup_logger

logger = setup_logger("SparkIngestion", "trip_load")

class SparkIngestionJob:
    """
    This class handles the heavy lifting of moving Taxi data from files into our Postgres database.
    """
    
    def __init__(self, target_date=None):
        self.target_date = target_date
        self.spark = None

    def _download_jdbc_driver(self):
        """Downloads the middleware driver for Postgres if we don't have it."""
        if not os.path.exists(JAR_DIR):
            os.makedirs(JAR_DIR)
        
        if not os.path.exists(JAR_PATH):
            logger.info(f"JDBC Driver not found. Downloading {JDBC_JAR_NAME}...")
            try:
                wget.download(JDBC_URL, out=JAR_PATH)
                print() # Newline after wget progress bar
                logger.info("Driver download successful.")
            except Exception as e:
                logger.error(f"Failed to download driver: {e}")
                sys.exit(1)
        else:
            logger.info(f"We already have the driver at {JAR_PATH}")

    def _create_spark_session(self):
        """Starts up Spark so we can process big data."""
        self.spark = SparkSession.builder \
            .appName("landing_to_stg_trips_daily_postgres") \
            .config("spark.jars", JAR_PATH) \
            .config("spark.driver.extraClassPath", JAR_PATH) \
            .master("local[*]") \
            .getOrCreate()
        logger.info("Spark is ready to go.")

    def _get_files_to_process(self):
        """Finds the files we need to work on."""
        files_to_process = []
        
        if self.target_date:
            # Look for the file in the specific daily folder
            daily_path = PathConfig.get_taxi_landing_path(self.target_date)
            file_pattern = f"yellow_tripdata_{self.target_date}.parquet"
            full_path = os.path.join(daily_path, file_pattern)
            
            if os.path.exists(full_path):
                logger.info(f"Daily Mode: Found file {full_path}")
                files_to_process.append(full_path)
            else:
                logger.warning(f"Daily Mode: File not found at {full_path}")
        else:
            # Batch Mode: Look everywhere in the landing folder
            root_dir = PathConfig.LANDING_TAXI
            if not os.path.exists(root_dir):
                logger.error(f"Landing Root not found: {root_dir}")
                return []
                
            logger.info(f"Batch Mode: Scanning {root_dir}...")
            for root, dirs, files in os.walk(root_dir):
                for file in files:
                    if file.endswith(".parquet"):
                        files_to_process.append(os.path.join(root, file))
        
        return files_to_process

    def _process_single_file(self, file_path):
        """Takes one file, reads it, and pushes it to Postgres."""
        file_name = os.path.basename(file_path)
        start_time = time.time()
        
        logger.info(f"=== Processing File: {file_name} ===")
        
        try:
            df = self.spark.read.parquet(file_path)
            
            row_count = df.count()
            logger.info(f"--> Data Volume: {row_count:,} rows.")
            
            if row_count == 0:
                logger.warning(f"--> No data to process in {file_name}.")
                return

            # Make all column names lowercase so they are consistent
            for col_name in df.columns:
                df = df.withColumnRenamed(col_name, col_name.lower())
            
            # Write to Postgres
            logger.info("--> Writing to Postgres (Table: raw.taxi_trips)...")
            df.write.jdbc(
                url=POSTGRES_URL,
                table="raw.taxi_trips",
                mode="append",
                properties=POSTGRES_PROPERTIES
            )
            
            duration = time.time() - start_time
            logger.info(f"--> SUCCESS: Loaded {file_name} in {duration:.2f} seconds.")
            
        except Exception as e:
            logger.error(f"--> FAILED to process {file_name}. Error: {e}")

    def run(self):
        """Main execution logic."""
        self._download_jdbc_driver()
        self._create_spark_session()
        
        files = self._get_files_to_process()
        
        if not files:
            logger.warning("No matching files found in Landing Zone.")
            self.spark.stop()
            return

        total_start_time = time.time()
        
        for file_name in files:
            self._process_single_file(file_name)
        
        total_duration = time.time() - total_start_time
        logger.info(f"=== Ingestion Complete. Total Time: {total_duration:.2f} seconds ===")
        
        self.spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest Taxi Data to Postgres')
    parser.add_argument('--date', type=str, help='Specific date to process (YYYY-MM-DD)', default=None)
    args = parser.parse_args()
    
    job = SparkIngestionJob(target_date=args.date)
    job.run()
