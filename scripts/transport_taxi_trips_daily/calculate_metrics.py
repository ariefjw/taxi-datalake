import logging
import os
import sys
import argparse
import time
import wget
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, avg, round, lit, sum as _sum

from scripts.common.path_utils import PathConfig

# Constants & Configuration
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

# Logging Setup
class CustomFormatter(logging.Formatter):
    """Custom log formatter for improved readability."""
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    green = "\x1b[32;20m"
    reset = "\x1b[0m"
    format_str = "[%(levelname)s] [%(asctime)s] %(message)s"

    def format(self, record):
        log_fmt = self.format_str
        if record.levelno == logging.INFO:
            log_fmt = self.green + self.format_str + self.reset
        elif record.levelno == logging.WARNING:
            log_fmt = self.yellow + self.format_str + self.reset
        elif record.levelno == logging.ERROR:
            log_fmt = self.red + self.format_str + self.reset
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)

def setup_logger(name="MetricsJob"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(CustomFormatter())
        logger.addHandler(ch)
    return logger

logger = setup_logger()

class TripMetricsJob:
    """
    Figures out daily stats (like average speed) from the taxi data using Spark.
    """
    
    def __init__(self, target_date):
        self.target_date = target_date
        self.spark = None

    def _ensure_jdbc_driver(self):
        """Downloads the Postgres driver if it's missing."""
        if not os.path.exists(JAR_DIR):
            os.makedirs(JAR_DIR)
        
        if not os.path.exists(JAR_PATH):
            logger.info(f"JDBC Driver not found. Downloading {JDBC_JAR_NAME}...")
            try:
                wget.download(JDBC_URL, out=JAR_PATH)
                print() 
                logger.info("Driver download successful.")
            except Exception as e:
                logger.error(f"Failed to download driver: {e}")
                sys.exit(1)
        else:
            logger.info(f"We already have the driver at {JAR_PATH}")

    def _create_spark_session(self):
        """Starts up the Spark engine."""
        self.spark = SparkSession.builder \
            .appName("calculate_trip_metrics") \
            .config("spark.jars", JAR_PATH) \
            .config("spark.driver.extraClassPath", JAR_PATH) \
            .master("local[*]") \
            .getOrCreate()
        logger.info("Spark is ready.")

    def _get_file_path(self):
        """Finds the daily file we need to analyze."""
        daily_path = PathConfig.get_taxi_landing_path(self.target_date)
        file_pattern = f"yellow_tripdata_{self.target_date}.parquet"
        full_path = os.path.join(daily_path, file_pattern)
        
        if os.path.exists(full_path):
            return full_path
        else:
            logger.error(f"File not found: {full_path}")
            return None

    def run(self):
        self._ensure_jdbc_driver()
        self.spark_session = self._create_spark_session()
        
        file_path = self._get_file_path()
        if not file_path:
            self.spark.stop()
            sys.exit(1)
            
        logger.info(f"Processing metrics for: {self.target_date}")
        
        try:
            df = self.spark.read.parquet(file_path)
            
            # Make sure column names are consistent
            for col_name in df.columns:
                df = df.withColumnRenamed(col_name, col_name.lower())
                
            # Check if we have the columns we really need
            required_cols = {'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance'}
            if not required_cols.issubset(set(df.columns)):
                logger.error(f"Missing required columns in dataset. Found: {df.columns}")
                self.spark.stop()
                sys.exit(1)
            
            # Calculate how long the trip took in seconds
            df = df.withColumn("duration_sec", 
                               unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime"))
            
            # Remove bad data (like negative time or distance)
            df_clean = df.filter((col("duration_sec") > 0) & (col("trip_distance") >= 0))
            
            # Crunch the numbers: total distance, total time, averages
            stats = df_clean.agg(
                _sum("trip_distance").alias("total_distance"),
                _sum("duration_sec").alias("total_duration"),
                avg("trip_distance").alias("avg_trip_distance"),
                avg("duration_sec").alias("avg_trip_duration")
            ).collect()[0]
            
            total_dist = stats["total_distance"] or 0.0
            total_dur = stats["total_duration"] or 0.0
            avg_dist = stats["avg_trip_distance"] or 0.0
            avg_dur = stats["avg_trip_duration"] or 0.0
            
            # Calculate average speed in MPH
            total_hours = total_dur / 3600.0
            avg_speed_mph = (total_dist / total_hours) if total_hours > 0 else 0.0
            
            logger.info(f"Metrics Calculated: Dist={avg_dist:.2f}, Dur={avg_dur:.2f}s, Speed={avg_speed_mph:.2f}mph")
            
            # Package the results nicely
            metrics_data = [(
                self.target_date,
                float(avg_dist),
                float(avg_dur),
                float(avg_speed_mph)
            )]
            
            schema = ["date", "avg_trip_distance", "avg_trip_duration_sec", "avg_speed_mph"]
            result_df = self.spark.createDataFrame(metrics_data, schema=schema)
            
            # Save the results to Postgres
            logger.info("Writing metrics to Postgres (Table: trip_metrics_daily)...")
            result_df.write.jdbc(
                url=POSTGRES_URL,
                table="trip_metrics_daily",
                mode="append",
                properties=POSTGRES_PROPERTIES
            )
            
            logger.info("Successfully wrote metrics.")
            
        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            sys.exit(1)
        finally:
            self.spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, required=True, help='Date to process (YYYY-MM-DD)')
    args = parser.parse_args()
    
    job = TripMetricsJob(args.date)
    job.run()
