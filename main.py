import sqlite3
import os
import logging
from datetime import datetime
from typing import List, Dict
import pandas as pd

# Configure logging for tracking script activities
logging.basicConfig(
    filename="main.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class LogProcessor:
    def __init__(self, db_name="logs.db"):
        """
        Initializes the LogProcessor with a connection to the SQLite database.
        
        Args:
            db_name (str): The name of the SQLite database (default is 'logs.db').
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        """Creates the logs table in the database if it does not already exist."""
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                log_level TEXT,
                message TEXT
            )
        """)
        self.conn.commit()

    def insert_logs(self, logs: List[Dict[str, str]]):
        """
        Inserts logs into the database in batches.
        
        Args:
            logs (List[Dict[str, str]]): List of log dictionaries to be inserted.
        """
        if logs:
            self.cursor.executemany("""
                INSERT INTO logs (timestamp, log_level, message)
                VALUES (:timestamp, :log_level, :message)
            """, logs)
            self.conn.commit()
            logging.info(f"Inserted {len(logs)} logs into the database.")
        else:
            logging.warning("No logs to insert.")

    def fetch_logs(self, limit: int = 100, offset: int = 0) -> List[Dict[str, str]]:
        """
        Fetches logs from the database with pagination support.
        
        Args:
            limit (int): The maximum number of logs to fetch (default is 100).
            offset (int): The number of logs to skip (for pagination).
            
        Returns:
            List[Dict[str, str]]: List of fetched logs.
        """
        self.cursor.execute("""
            SELECT timestamp, log_level, message
            FROM logs
            LIMIT ? OFFSET ?
        """, (limit, offset))
        rows = self.cursor.fetchall()
        return [{"timestamp": row[0], "log_level": row[1], "message": row[2]} for row in rows]

    def clean_logs(self, exclude_keywords: List[str], exclude_log_levels: List[str]):
        """
        Clean logs by excluding certain keywords and log levels.
        
        Args:
            exclude_keywords (List[str]): List of keywords to exclude from logs.
            exclude_log_levels (List[str]): List of log levels to exclude from logs.
        """
        self.cursor.execute("""
            DELETE FROM logs
            WHERE log_level IN ({seq}) OR message LIKE ({keywords})
        """.format(
            seq=','.join(['?'] * len(exclude_log_levels)),
            keywords=" OR ".join([f" ? " for _ in exclude_keywords])
        ), (*exclude_log_levels, *exclude_keywords))
        self.conn.commit()
        logging.info("Logs cleaned based on the provided keywords and log levels.")

    def count_logs(self) -> int:
        """Returns the count of logs currently in the database."""
        self.cursor.execute("SELECT COUNT(*) FROM logs")
        return self.cursor.fetchone()[0]

    def close(self):
        """Close the database connection."""
        self.conn.close()


class LogAnalysis:
    def __init__(self, log_processor: LogProcessor):
        """
        Initializes LogAnalysis to process and analyze logs.

        Args:
            log_processor (LogProcessor): The LogProcessor instance for database operations.
        """
        self.processor = log_processor

    def process_log_file(self, file_path: str, chunk_size: int = 1000):
        """
        Processes a log file in chunks and inserts logs into the database.
        
        Args:
            file_path (str): The path to the log file to process.
            chunk_size (int): The number of lines to process per chunk (default 1000).
        """
        logs = []
        with open(file_path, 'r') as file:
            for line in file:
                # Example parsing: timestamp, log_level, message
                parts = line.strip().split(" ", 2)
                if len(parts) >= 3:
                    timestamp, log_level, message = parts
                    logs.append({
                        "timestamp": timestamp,
                        "log_level": log_level,
                        "message": message
                    })

                # Insert logs into database in chunks
                if len(logs) >= chunk_size:
                    self.processor.insert_logs(logs)
                    logs = []  # Reset logs after each chunk insertion

        # Insert any remaining logs in the last batch
        if logs:
            self.processor.insert_logs(logs)

    def export_to_csv(self, output_file: str):
        """
        Exports logs from the database to a CSV file.
        
        Args:
            output_file (str): The path where the CSV file will be saved.
        """
        logs = self.processor.fetch_logs(limit=1000)
        df = pd.DataFrame(logs)
        df.to_csv(output_file, index=False)
        logging.info(f"Exported logs to {output_file}")


if __name__ == "__main__":
    # Set up the log processor and analysis objects
    log_processor = LogProcessor(db_name="logs.db")
    log_analysis = LogAnalysis(log_processor=log_processor)

    try:
        # Process a log file
        log_file_path = "large_log_file.txt"  # Replace with your actual log file
        log_analysis.process_log_file(file_path=log_file_path)

        # Clean logs based on specific criteria (example: exclude DEBUG logs)
        log_analysis.processor.clean_logs(exclude_keywords=["health_check", "debug"], exclude_log_levels=["INFO"])

        # Export processed logs to CSV
        log_analysis.export_to_csv("processed_logs.csv")

    except Exception as e:
        logging.error(f"Error during log processing: {e}")

    finally:
        log_processor.close()
