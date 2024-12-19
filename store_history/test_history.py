import sqlite3
import os
import logging
import unittest
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple

# Configure logging for better test tracking
logging.basicConfig(
    filename="test_history.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class TestHistory:
    def __init__(self, db_name: str = "logs.db"):
        """
        Initialize the test history object with a SQLite database.
        
        Args:
            db_name (str): Name of the SQLite database file.
        """
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self._create_table()

    def _create_table(self):
        """
        Create a table to store log history if it doesn't exist.
        """
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
        Insert a list of logs into the database in batch.

        Args:
            logs (List[Dict[str, str]]): List of logs to insert. Each log should have 
                                         'timestamp', 'log_level', and 'message' keys.
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
        Fetch logs from the database with pagination support.
        
        Args:
            limit (int): The maximum number of logs to fetch.
            offset (int): The number of logs to skip for pagination.
            
        Returns:
            List[Dict[str, str]]: List of logs from the database.
        """
        self.cursor.execute("""
            SELECT timestamp, log_level, message
            FROM logs
            LIMIT ? OFFSET ?
        """, (limit, offset))
        rows = self.cursor.fetchall()
        return [{"timestamp": row[0], "log_level": row[1], "message": row[2]} for row in rows]

    def delete_old_logs(self, days: int):
        """
        Delete logs older than a specified number of days.
        
        Args:
            days (int): The number of days to retain logs.
        """
        cutoff_date = (datetime.now() - pd.Timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
        self.cursor.execute("""
            DELETE FROM logs WHERE timestamp < ?
        """, (cutoff_date,))
        self.conn.commit()
        logging.info(f"Deleted logs older than {cutoff_date}.")

    def count_logs(self) -> int:
        """
        Count the number of logs in the database.
        
        Returns:
            int: The total number of logs.
        """
        self.cursor.execute("SELECT COUNT(*) FROM logs")
        count = self.cursor.fetchone()[0]
        return count

    def close(self):
        """Close the database connection."""
        self.conn.close()


class TestHistoryOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up the test by creating a new database instance."""
        cls.db_name = "test_logs.db"
        cls.history = TestHistory(db_name=cls.db_name)

    @classmethod
    def tearDownClass(cls):
        """Clean up after tests by removing the database file."""
        if os.path.exists(cls.db_name):
            os.remove(cls.db_name)
            logging.info(f"Database {cls.db_name} removed.")

    def test_insert_logs(self):
        """Test inserting logs into the database."""
        logs = [
            {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "log_level": "INFO", "message": "Test log 1"},
            {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "log_level": "ERROR", "message": "Test log 2"}
        ]
        self.history.insert_logs(logs)
        count = self.history.count_logs()
        self.assertEqual(count, 2, "Log count should be 2 after insertion.")
        logging.info("Test insert logs passed.")

    def test_fetch_logs(self):
        """Test fetching logs from the database."""
        logs = self.history.fetch_logs(limit=2)
        self.assertEqual(len(logs), 2, "Fetched logs count should be 2.")
        self.assertTrue("timestamp" in logs[0], "'timestamp' should be a key in the log entry.")
        self.assertTrue("log_level" in logs[0], "'log_level' should be a key in the log entry.")
        self.assertTrue("message" in logs[0], "'message' should be a key in the log entry.")
        logging.info("Test fetch logs passed.")

    def test_delete_old_logs(self):
        """Test deleting old logs."""
        self.history.delete_old_logs(days=30)
        count = self.history.count_logs()
        self.assertGreater(count, 0, "There should be logs remaining after deletion.")
        logging.info("Test delete old logs passed.")

    def test_batch_processing(self):
        """Test batch processing of logs."""
        logs = [{"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "log_level": "DEBUG", "message": f"Log entry {i}"} for i in range(10000)]
        batch_size = 500
        for i in range(0, len(logs), batch_size):
            self.history.insert_logs(logs[i:i+batch_size])

        # Verify total log count
        count = self.history.count_logs()
        self.assertEqual(count, 10000, "Log count should be 10000 after batch insertion.")
        logging.info("Test batch processing passed.")

    def test_export_to_dataframe(self):
        """Test exporting logs to a Pandas DataFrame."""
        logs = self.history.fetch_logs(limit=10)
        df = pd.DataFrame(logs)
        self.assertEqual(len(df), 10, "The DataFrame should contain 10 rows.")
        self.assertIn("timestamp", df.columns, "'timestamp' column should exist.")
        self.assertIn("log_level", df.columns, "'log_level' column should exist.")
        self.assertIn("message", df.columns, "'message' column should exist.")
        logging.info("Test export to DataFrame passed.")


if __name__ == "__main__":
    unittest.main()
