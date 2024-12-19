import sqlite3
import logging
from typing import List, Dict
import os

# Set up logging
logging.basicConfig(
    filename="db_setup.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database File
DB_FILE = "log_analysis.db"

def create_db_connection():
    """
    Create a connection to the SQLite database. If the database file doesn't exist,
    it will be created.
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        logging.info(f"Connected to database {DB_FILE}")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def create_table():
    """
    Create the necessary table for storing log analysis results in the database.
    """
    conn = create_db_connection()
    cursor = conn.cursor()

    # Create table if it doesn't exist
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

def create_index():
    """
    Create index for performance improvement in querying by timestamp and log entry.
    """
    conn = create_db_connection()
    cursor = conn.cursor()

    create_index_query = """
    CREATE INDEX IF NOT EXISTS idx_log_timestamp ON LogAnalysisHistory (timestamp);
    CREATE INDEX IF NOT EXISTS idx_log_entry ON LogAnalysisHistory (log_entry);
    """
    cursor.execute(create_index_query)
    conn.commit()
    logging.info("Indexes created for 'timestamp' and 'log_entry'.")
    conn.close()

def store_analysis_result(log_entry: str, root_cause: str):
    """
    Store a single log analysis result into the database.
    
    Args:
        log_entry (str): The original log message.
        root_cause (str): The root cause identified by GPT.
    """
    try:
        conn = create_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO LogAnalysisHistory (log_entry, root_cause)
        VALUES (?, ?);
        """
        cursor.execute(insert_query, (log_entry, root_cause))
        conn.commit()
        logging.info(f"Stored log analysis: {log_entry[:30]}...")
        conn.close()
    except Exception as e:
        logging.error(f"Error storing log analysis: {e}")
        raise

def store_batch_analysis_results(batch_results: List[Dict[str, str]]):
    """
    Store a batch of log analysis results into the database in one transaction.
    
    Args:
        batch_results (List[Dict[str, str]]): A list of log entries with root cause analysis.
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
        raise

def fetch_all_history(limit: int = 1000) -> List[Dict[str, str]]:
    """
    Fetch all the analysis results from the history table, optionally limiting the number of results.
    
    Args:
        limit (int): Maximum number of results to return (default: 1000).
    
    Returns:
        List[Dict[str, str]]: A list of dictionaries containing log entries and their corresponding root causes.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    fetch_query = "SELECT id, log_entry, root_cause, timestamp FROM LogAnalysisHistory LIMIT ?;"
    cursor.execute(fetch_query, (limit,))
    
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
    Fetch a specific log analysis by its ID.
    
    Args:
        log_id (int): The ID of the log entry to retrieve.
    
    Returns:
        Dict[str, str]: The log entry and its root cause, or an empty dictionary if not found.
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
    Delete log analysis results older than a specified number of days.
    
    Args:
        older_than_days (int): The number of days before which entries should be deleted (default: 30 days).
    """
    conn = create_db_connection()
    cursor = conn.cursor()

    delete_query = """
    DELETE FROM LogAnalysisHistory WHERE timestamp < DATE('now', ?);
    """
    cursor.execute(delete_query, (f'-{older_than_days} days',))
    conn.commit()
    logging.info(f"Deleted entries older than {older_than_days} days.")
    conn.close()

def count_entries() -> int:
    """
    Get the total number of entries in the LogAnalysisHistory table.
    
    Returns:
        int: Total number of log entries in the database.
    """
    conn = create_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM LogAnalysisHistory;")
    count = cursor.fetchone()[0]
    
    conn.close()
    return count

if __name__ == "__main__":
    create_table()
    create_index()
