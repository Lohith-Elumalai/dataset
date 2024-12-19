import re
import logging
import pandas as pd
from typing import List, Dict, Generator
import os
from datetime import datetime

# Set up logging for structuring
logging.basicConfig(
    filename="structurer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Regex patterns for log parsing (example)
LOG_PATTERN = re.compile(r'(?P<timestamp>\S+\s+\S+)\s+(?P<log_level>\S+)\s+(?P<message>.+)')

# Function to read logs in chunks to handle large files
def read_large_log_file(file_path: str, chunk_size: int = 1000) -> Generator[str, None, None]:
    """
    Reads a large log file line by line in chunks to avoid memory overload.
    
    Args:
        file_path (str): Path to the log file.
        chunk_size (int): The number of lines to process at once.
        
    Yields:
        str: The next chunk of log lines.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        chunk = []
        for line in file:
            chunk.append(line.strip())
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

# Log filtering and data structuring
def filter_and_structure_logs(log_lines: List[str]) -> List[Dict[str, str]]:
    """
    Filters and structures the log lines into a list of dictionaries based on the defined pattern.
    
    Args:
        log_lines (List[str]): The list of log lines to process.
    
    Returns:
        List[Dict[str, str]]: A structured list of log entries with timestamp, log level, and message.
    """
    structured_logs = []
    
    for line in log_lines:
        match = LOG_PATTERN.match(line)
        if match:
            log_entry = match.groupdict()
            structured_logs.append(log_entry)
        else:
            logging.warning(f"Skipped malformed log entry: {line}")
    
    return structured_logs

# Function to clean logs (e.g., remove irrelevant logs based on log level or keywords)
def clean_logs(logs: List[Dict[str, str]], exclude_keywords: List[str] = None, exclude_log_levels: List[str] = None) -> List[Dict[str, str]]:
    """
    Cleans the logs by removing entries based on specified keywords or log levels.
    
    Args:
        logs (List[Dict[str, str]]): List of structured log entries.
        exclude_keywords (List[str]): List of keywords to exclude from logs.
        exclude_log_levels (List[str]): List of log levels to exclude.
    
    Returns:
        List[Dict[str, str]]: The cleaned list of logs.
    """
    if exclude_keywords is None:
        exclude_keywords = []
    if exclude_log_levels is None:
        exclude_log_levels = []
    
    cleaned_logs = [
        log for log in logs
        if not any(keyword in log['message'] for keyword in exclude_keywords) 
        and log['log_level'] not in exclude_log_levels
    ]
    
    logging.info(f"Cleaned {len(logs) - len(cleaned_logs)} logs based on filter criteria.")
    return cleaned_logs

# Function to store structured logs into a dataframe for easy further analysis
def store_logs_in_dataframe(logs: List[Dict[str, str]]) -> pd.DataFrame:
    """
    Converts the structured log entries into a pandas DataFrame for further analysis.
    
    Args:
        logs (List[Dict[str, str]]): The structured log entries.
    
    Returns:
        pd.DataFrame: A DataFrame containing the structured log data.
    """
    if logs:
        df = pd.DataFrame(logs)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        logging.info(f"Structured {len(df)} logs into DataFrame.")
        return df
    else:
        logging.warning("No logs to structure into DataFrame.")
        return pd.DataFrame()

# Function to process a large log file, clean and structure it
def process_large_log_file(file_path: str, chunk_size: int = 1000, exclude_keywords: List[str] = None, exclude_log_levels: List[str] = None) -> pd.DataFrame:
    """
    Processes a large log file in chunks, cleans and structures it into a pandas DataFrame.
    
    Args:
        file_path (str): Path to the log file.
        chunk_size (int): The number of lines to process at once.
        exclude_keywords (List[str]): List of keywords to exclude from logs.
        exclude_log_levels (List[str]): List of log levels to exclude.
    
    Returns:
        pd.DataFrame: A DataFrame containing the structured log data.
    """
    structured_logs = []
    
    for chunk in read_large_log_file(file_path, chunk_size):
        logging.info(f"Processing chunk with {len(chunk)} log entries.")
        structured_chunk = filter_and_structure_logs(chunk)
        cleaned_chunk = clean_logs(structured_chunk, exclude_keywords, exclude_log_levels)
        structured_logs.extend(cleaned_chunk)
    
    # Once all logs are processed, store them in a DataFrame
    df = store_logs_in_dataframe(structured_logs)
    return df

# Example function to save the structured DataFrame to CSV
def save_dataframe_to_csv(df: pd.DataFrame, output_file: str):
    """
    Saves the structured logs DataFrame to a CSV file.
    
    Args:
        df (pd.DataFrame): The DataFrame to save.
        output_file (str): Path where to save the CSV file.
    """
    if not df.empty:
        df.to_csv(output_file, index=False)
        logging.info(f"Structured data saved to {output_file}")
    else:
        logging.warning("No data to save.")
