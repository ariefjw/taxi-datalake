import os

class PathConfig:
    # Base Airflow Path (in container)
    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
    
    # Data Root
    DATA_ROOT = os.path.join(AIRFLOW_HOME, "data")
    
    # Landing Zones
    LANDING_TAXI = os.path.join(DATA_ROOT, "taxi", "trips")
    LANDING_WEATHER = os.path.join(DATA_ROOT, "weather", "city")
    
    # Logs Directory
    LOGS_DIR = os.path.join(DATA_ROOT, "logs")
    
    # Script Paths (for Airflow DAG references)
    SCRIPT_ROOT = os.path.join(AIRFLOW_HOME, "scripts")
    
    @staticmethod
    def get_taxi_landing_path(date_str):
        """Returns: /opt/airflow/data/landing/taxi/trips/YYYY-MM-DD/"""
        return os.path.join(PathConfig.LANDING_TAXI, date_str)

    @staticmethod
    def get_weather_landing_path(city, date_str):
        """Returns: /opt/airflow/data/landing/weather/city/YYYY-MM-DD/"""
        return os.path.join(PathConfig.LANDING_WEATHER, city, date_str)
