import sqlite3
import logging
from typing import List, Dict
import os

# Set up logging
logging.basicConfig(
    filename="history_manager.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define the database file path
DB_FILE = "log_analysis_history.db"

def create_db_connection():
    """
    Create a connection to the SQLite database. If the database file doesn't exist,
    it will be created.
    """
    conn = sqlite3.connect(DB_FILE)
    logging.info(f"Connected to the database {DB_FILE}")
    return conn

def create_table():
    """
    Create the table for storing log analysis results if it doesn't already exist.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    # SQL query to create the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS LogAnalysisHistory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        log_entry TEXT NOT NULL,
        root_cause TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    logging.info("Table 'LogAnalysisHistory' is created or already exists.")
    conn.close()

def store_analysis_result(log_entry: str, root_cause: str):
    """
    Store the result of the log analysis into the database.
    
    Args:
        log_entry (str): The original log entry.
        root_cause (str): The root cause identified by GPT.
    """
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        
        # SQL query to insert the analysis result into the table
        insert_query = """
        INSERT INTO LogAnalysisHistory (log_entry, root_cause)
        VALUES (?, ?);
        """
        
        cursor.execute(insert_query, (log_entry, root_cause))
        conn.commit()
        logging.info(f"Successfully stored log analysis for: {log_entry[:30]}...")
        conn.close()
    except Exception as e:
        logging.error(f"Error storing log analysis: {e}")

def fetch_all_history() -> List[Dict[str, str]]:
    """
    Fetch all the analysis results from the history table.
    
    Returns:
        List[Dict[str, str]]: A list of dictionaries containing the logs and their corresponding root causes.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    fetch_query = "SELECT id, log_entry, root_cause, timestamp FROM LogAnalysisHistory;"
    cursor.execute(fetch_query)
    
    rows = cursor.fetchall()
    conn.close()
    
    history = []
    for row in rows:
        history.append({
            "id": row[0],
            "log_entry": row[1],
            "root_cause": row[2],
            "timestamp": row[3]
        })
    
    logging.info(f"Fetched {len(history)} historical entries.")
    return history

def fetch_history_by_id(log_id: int) -> Dict[str, str]:
    """
    Fetch the details of a specific log analysis by its ID.
    
    Args:
        log_id (int): The ID of the log entry to retrieve.
    
    Returns:
        Dict[str, str]: A dictionary containing the log and its root cause.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    fetch_query = "SELECT log_entry, root_cause, timestamp FROM LogAnalysisHistory WHERE id = ?;"
    cursor.execute(fetch_query, (log_id,))
    
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {
            "log_entry": row[0],
            "root_cause": row[1],
            "timestamp": row[2]
        }
    else:
        logging.warning(f"No entry found for ID: {log_id}")
        return {}

def delete_old_history(older_than_days: int = 30):
    """
    Delete historical log analysis results older than a specified number of days.
    
    Args:
        older_than_days (int): The age in days after which the log entries should be deleted.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    delete_query = """
    DELETE FROM LogAnalysisHistory 
    WHERE timestamp < DATE('now', ?);
    """
    
    cursor.execute(delete_query, (f'-{older_than_days} days',))
    conn.commit()
    logging.info(f"Deleted entries older than {older_than_days} days.")
    conn.close()

def store_batch_analysis_results(batch_results: List[Dict[str, str]]):
    """
    Store multiple analysis results in the database in a single transaction.
    
    Args:
        batch_results (List[Dict[str, str]]): A list of dictionaries containing logs and their root causes.
    """
    try:
        conn = create_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO LogAnalysisHistory (log_entry, root_cause)
        VALUES (?, ?);
        """
        
        cursor.executemany(insert_query, [(result['log'], result['root_cause']) for result in batch_results])
        conn.commit()
        logging.info(f"Successfully stored {len(batch_results)} batch log analyses.")
        conn.close()
    except Exception as e:
        logging.error(f"Error storing batch log analysis: {e}")
