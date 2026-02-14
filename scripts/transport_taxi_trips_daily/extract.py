import os
import logging
import datetime
import pandas as pd
from typing import Optional

# Configuration
from scripts.common.path_utils import PathConfig
SOURCE_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet"

# Logging Setup
class CustomFormatter(logging.Formatter):
    """Custom log formatter to match Spark script style."""
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

def setup_logger(name="TripIngestion"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(CustomFormatter())
        logger.addHandler(ch)
    return logger

logger = setup_logger()

def download_data_for_date(execution_date: str) -> str:
    """
    Downloads taxi data for a specific date (YYYY-MM-DD).
    
    Args:
        execution_date: The date we want data for (e.g., '2023-01-01').
        
    Returns:
        str: A message saying if it worked, adds the filename.
    """
    logger.info(f"Starting download for date: {execution_date}")
    
    try:
        # Split the date to get the year and month for the URL
        year, month, _ = execution_date.split('-')
        
        # Build the download URL and where we want to save the file
        url = SOURCE_URL_TEMPLATE.format(year, month)
        
        # Save it here: /data/landing/taxi/trips/YYYY-MM-DD/
        output_dir = PathConfig.get_taxi_landing_path(execution_date)
        os.makedirs(output_dir, exist_ok=True)
        
        output_filename = f"yellow_tripdata_{execution_date}.parquet"
        output_path = os.path.join(output_dir, output_filename)
        
        logger.info(f"Source URL: {url}")
        logger.info(f"Target Path: {output_path}")
        
        # Figure out the start and end of the day so we can filter the data
        target_date_dt = datetime.datetime.strptime(execution_date, "%Y-%m-%d").date()
        target_date_ts = pd.Timestamp(target_date_dt)
        next_date_ts = target_date_ts + pd.Timedelta(days=1)
        
        logger.info("Reading remote parquet with Pandas (PyArrow engine)...")
        
        # Grab the data from the web, but only for the specific day we want
        df = pd.read_parquet(
            url, 
            engine='pyarrow',
            filters=[
                ('tpep_pickup_datetime', '>=', target_date_ts),
                ('tpep_pickup_datetime', '<', next_date_ts)
            ]
        )
        
        row_count = len(df)
        logger.info(f"Rows fetched: {row_count}")
        
        if df.empty:
            logger.warning(f"No data found for {execution_date}")
        
        # Write the file to our local folder
        df.to_parquet(output_path, index=False)
        
        logger.info(f"SUCCESS: Saved daily file {output_filename}")
        return f"SUCCESS: {output_filename}"
        
    except Exception as e:
        error_msg = f"FAILED: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)