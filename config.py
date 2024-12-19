import os
from dotenv import load_dotenv

# Load environment variables from a .env file if present
load_dotenv()

# Define configuration for handling large datasets efficiently
class Config:
    # Database configuration
    DATABASE_URI = os.getenv("DATABASE_URI", "sqlite:///logs.db")  # SQLite URI by default
    DATABASE_POOL_SIZE = int(os.getenv("DATABASE_POOL_SIZE", 5))  # Maximum database connections
    DATABASE_MAX_OVERFLOW = int(os.getenv("DATABASE_MAX_OVERFLOW", 10))  # Max overflow connections

    # Log processing configuration
    LOG_BATCH_SIZE = int(os.getenv("LOG_BATCH_SIZE", 1000))  # Number of logs to process in one batch
    LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "large_log_file.txt")  # Path to the log file to process
    LOG_FILE_ENCODING = os.getenv("LOG_FILE_ENCODING", "utf-8")  # Encoding used in log file

    # Log configuration for handling system logs
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # Default log level
    LOG_FILE = os.getenv("LOG_FILE", "main.log")  # Path to save logs

    # Clean-up settings
    CLEAN_LOGS_OLDER_THAN_DAYS = int(os.getenv("CLEAN_LOGS_OLDER_THAN_DAYS", 30))  # Days threshold for cleaning old logs

    # Export settings
    EXPORT_CSV_PATH = os.getenv("EXPORT_CSV_PATH", "processed_logs.csv")  # Path to save the exported CSV file

    # Performance and scaling parameters
    MAX_ROWS_PER_QUERY = int(os.getenv("MAX_ROWS_PER_QUERY", 1000))  # Rows to fetch in each query
    COMPRESSION_LEVEL = int(os.getenv("COMPRESSION_LEVEL", 9))  # Compression level for CSV export


# Load configuration from the environment
config = Config()

