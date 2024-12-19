import sqlite3
import os
import logging
from datetime import datetime, timedelta
from typing import List

# Configure logging for tracking script activities
logging.basicConfig(
    filename="cleaner.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class LogCleaner:
    def __init__(self, db_name="logs.db"):
        """
        Initializes the LogCleaner with a connection to the SQLite database.
        
        Args:
            db_name (str): The name of the SQLite database (default is 'logs.db').
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        logging.info(f"Connected to database {db_name}.")

    def remove_old_logs(self, days_threshold: int = 30):
        """
        Removes logs older than a specified number of days.

        Args:
            days_threshold (int): The number of days used to determine which logs are old. Default is 30.
        """
        cutoff_date = datetime.now() - timedelta(days=days_threshold)
        cutoff_date_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        
        self.cursor.execute("""
            DELETE FROM logs
            WHERE timestamp < ?
        """, (cutoff_date_str,))
        
        self.conn.commit()
        logging.info(f"Removed logs older than {cutoff_date_str}.")

    def remove_irrelevant_logs(self, keywords: List[str], log_levels: List[str]):
        """
        Removes logs based on irrelevant keywords and log levels.
        
        Args:
            keywords (List[str]): List of keywords to search for and remove from logs.
            log_levels (List[str]): List of log levels to remove from logs.
        """
        keyword_conditions = " OR ".join([f"message LIKE ?" for _ in keywords])
        log_level_conditions = " OR ".join([f"log_level = ?" for _ in log_levels])

        query = f"""
            DELETE FROM logs
            WHERE ({keyword_conditions}) OR ({log_level_conditions})
        """

        # Prepare parameters for the query
        parameters = [f"%{keyword}%" for keyword in keywords] + log_levels

        self.cursor.execute(query, tuple(parameters))
        self.conn.commit()
        logging.info(f"Removed logs containing keywords {keywords} or with log levels {log_levels}.")

    def clean_logs_in_batches(self, batch_size: int = 1000):
        """
        Cleans logs by processing and filtering logs in batches.
        
        Args:
            batch_size (int): The size of each batch for processing (default is 1000).
        """
        offset = 0
        while True:
            # Fetch a batch of logs
            logs = self.fetch_logs(limit=batch_size, offset=offset)
            if not logs:
                break

            # Apply cleaning to the batch
            cleaned_logs = self.filter_logs(logs)

            # Re-insert cleaned logs back into the database
            self.reinsert_logs(cleaned_logs)

            # Update offset for the next batch
            offset += batch_size
            logging.info(f"Processed and cleaned batch starting from offset {offset}.")

    def fetch_logs(self, limit: int = 1000, offset: int = 0) -> List[dict]:
        """
        Fetches logs from the database in a paginated manner (useful for large datasets).
        
        Args:
            limit (int): The number of logs to fetch per query.
            offset (int): The offset for pagination.
        
        Returns:
            List[dict]: A list of logs.
        """
        self.cursor.execute("""
            SELECT id, timestamp, log_level, message
            FROM logs
            LIMIT ? OFFSET ?
        """, (limit, offset))
        
        rows = self.cursor.fetchall()
        return [{"id": row[0], "timestamp": row[1], "log_level": row[2], "message": row[3]} for row in rows]

    def filter_logs(self, logs: List[dict]) -> List[dict]:
        """
        Filters out logs based on certain criteria (e.g., irrelevant logs).
        
        Args:
            logs (List[dict]): A list of logs to be filtered.
        
        Returns:
            List[dict]: The filtered list of logs.
        """
        filtered_logs = []

        for log in logs:
            # Example condition: Keep only logs that do not contain the keyword 'debug' or are not 'INFO' level
            if "debug" not in log["message"].lower() and log["log_level"].upper() != "INFO":
                filtered_logs.append(log)

        return filtered_logs

    def reinsert_logs(self, logs: List[dict]):
        """
        Re-inserts cleaned logs back into the database.

        Args:
            logs (List[dict]): A list of cleaned logs to insert.
        """
        if logs:
            self.cursor.executemany("""
                INSERT OR REPLACE INTO logs (id, timestamp, log_level, message)
                VALUES (:id, :timestamp, :log_level, :message)
            """, logs)
            self.conn.commit()
            logging.info(f"Re-inserted {len(logs)} cleaned logs into the database.")

    def close(self):
        """Close the database connection."""
        self.conn.close()
        logging.info(f"Closed the connection to the database {self.db_name}.")


if __name__ == "__main__":
    # Initialize LogCleaner
    log_cleaner = LogCleaner(db_name="logs.db")

    try:
        # Clean old logs (older than 30 days by default)
        log_cleaner.remove_old_logs(days_threshold=30)

        # Clean logs based on irrelevant keywords and log levels
        log_cleaner.remove_irrelevant_logs(
            keywords=["health_check", "debug"],
            log_levels=["INFO", "DEBUG"]
        )

        # Process logs in batches and clean them
        log_cleaner.clean_logs_in_batches(batch_size=1000)

    except Exception as e:
        logging.error(f"Error during log cleaning: {e}")

    finally:
        # Close the database connection
        log_cleaner.close()
