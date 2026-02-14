import logging
import requests
import pandas as pd
import argparse
import os
import sys
from sqlalchemy import create_engine, text

# Setup Logger
class CustomFormatter(logging.Formatter):
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

def setup_logger(name="WeatherIngest"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(CustomFormatter())
        logger.addHandler(ch)
    return logger

logger = setup_logger()

# Configuration
from scripts.common.path_utils import PathConfig

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = "5432"

if not all([POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB]):
     raise ValueError("Missing required database environment variables.")

DB_CONNECTION_STR = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DB_HOST}:{DB_PORT}/{POSTGRES_DB}"

def fetch_weather(date):
    """
    Grabs hourly weather data from the Open-Meteo API.
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": date,
        "end_date": date,
        "hourly": ["temperature_2m", "precipitation", "snowfall"],
        "timezone": "America/New_York"
    }
    
    logger.info(f"Fetching hourly weather data for {date} from {url}...")
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if 'hourly' not in data:
            logger.error("No 'hourly' key in API response.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data['hourly'])
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        sys.exit(1)

def save_parquet(df, date):
    """
    Saves the data as a Parquet file so we don't lose it.
    """
    # Use standardized path: /data/landing/weather/nyc/YYYY-MM-DD/
    landing_path = PathConfig.get_weather_landing_path("nyc", date)
    
    if not os.path.exists(landing_path):
        try:
            os.makedirs(landing_path, exist_ok=True)
        except OSError as e:
            logger.warning(f"Could not create {landing_path}: {e}")
            sys.exit(1)

    filename = f"weather_hourly_{date}.parquet"
    filepath = os.path.join(landing_path, filename)
    
    try:
        df.to_parquet(filepath, index=False)
        logger.info(f"Saved parquet file to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Failed to save parquet: {e}")
        sys.exit(1)

def load_to_postgres(df):
    """
    Takes the dataframe and puts it into Postgres.
    """
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # Make sure the 'raw' schema is there before we try to write to it
        with engine.begin() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

        
        # Rename columns so they match what the database expects
        # Schema: timestamp, temp_c, precipitation_mm, snowfall_cm
        df_transformed = df.rename(columns={
            "time": "timestamp",
            "temperature_2m": "temp_c",
            "precipitation": "precipitation_mm",
            "snowfall": "snowfall_cm"
        })
        
        required_cols = ["timestamp", "temp_c", "precipitation_mm", "snowfall_cm"]
        # Double check we have all the columns, fill with nothing if missing
        for col in required_cols:
            if col not in df_transformed.columns:
                df_transformed[col] = None 
                
        df_final = df_transformed[required_cols]

        logger.info("Loading data into Postgres table 'raw.weather_hourly'...")
        df_final.to_sql('weather_hourly', engine, schema='raw', if_exists='append', index=False)
        logger.info("Successfully loaded data to Postgres.")
        
    except Exception as e:
        logger.error(f"Failed to load to Postgres: {e}")
        sys.exit(1)

def main(date):
    logger.info(f"=== Starting Weather Ingestion for {date} ===")
    
    # 1. Go get the data
    df = fetch_weather(date)
    if df.empty:
        logger.warning(f"No weather data available for {date}.")
        sys.exit(0)
    
    # 2. Save it locally
    save_parquet(df, date)
    
    # 3. Push it to the database
    load_to_postgres(df)
    
    logger.info("=== Ingestion Complete ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest Weather Data to Postgres')
    parser.add_argument('--date', type=str, required=True, help='Date to process (YYYY-MM-DD)')
    args = parser.parse_args()
    
    main(args.date)
