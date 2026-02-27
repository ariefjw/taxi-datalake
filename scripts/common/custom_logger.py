import logging
from logging.handlers import RotatingFileHandler
import os
import sys
from scripts.common.path_utils import PathConfig

class CustomFormatter(logging.Formatter):
    """Custom log formatter for improved readability with colors for console."""
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


def setup_logger(name, log_filename):
    """
    Configures and returns a logger instance that logs to both stdout and a rolling file.
    
    Args:
        name: Name of the logger to retrieve (e.g. 'TripIngestion')
        log_filename: Name of the log file to generate (without .log extension)
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Check if handlers already exist to avoid duplicate logs in same session
    if not logger.handlers:
        # Console Handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(CustomFormatter())
        logger.addHandler(ch)
        
        # File Handler setup
        log_dir = PathConfig.LOGS_DIR
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except Exception as e:
                # If we fail to make the directory, console output is enough
                logger.warning(f"Could not create log directory {log_dir}: {e}")
                return logger
                
        log_path = os.path.join(log_dir, f"{log_filename}.log")
        
        # Plain formatter without colors for files
        file_formatter = logging.Formatter(
            "[%(levelname)s] [%(asctime)s] [%(name)s] %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Keep up to 5 history files of 10MB each
        fh = RotatingFileHandler(log_path, maxBytes=10*1024*1024, backupCount=5)
        fh.setLevel(logging.INFO)
        fh.setFormatter(file_formatter)
        logger.addHandler(fh)

    return logger
