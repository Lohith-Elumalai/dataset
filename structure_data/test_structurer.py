import unittest
import os
import pandas as pd
from datetime import datetime
from structurer import read_large_log_file, filter_and_structure_logs, clean_logs, process_large_log_file, save_dataframe_to_csv


class TestLogProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Set up test class by creating a large synthetic log file.
        """
        cls.log_file = "test_large_log.txt"
        cls.create_large_log_file(cls.log_file, num_entries=10000)
        cls.exclude_keywords = ["health_check", "debug"]
        cls.exclude_log_levels = ["INFO"]
    
    @classmethod
    def tearDownClass(cls):
        """
        Clean up test class by deleting the generated log file.
        """
        if os.path.exists(cls.log_file):
            os.remove(cls.log_file)

    @staticmethod
    def create_large_log_file(file_path: str, num_entries: int):
        """
        Creates a synthetic large log file with the given number of entries.
        
        Args:
            file_path (str): Path to save the log file.
            num_entries (int): Number of log entries to write.
        """
        log_levels = ["INFO", "ERROR", "DEBUG", "WARNING"]
        with open(file_path, 'w', encoding='utf-8') as file:
            for i in range(num_entries):
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_level = log_levels[i % 4]
                message = f"Test log message {i} with {log_level}"
                file.write(f"{timestamp} {log_level} {message}\n")
        print(f"Large log file '{file_path}' created with {num_entries} entries.")

    def test_read_large_log_file(self):
        """
        Test the ability to read large log files in chunks.
        """
        chunk_size = 1000
        chunk_count = 0
        for chunk in read_large_log_file(self.log_file, chunk_size):
            chunk_count += 1
            self.assertTrue(len(chunk) <= chunk_size, "Chunk size exceeded.")
        self.assertGreater(chunk_count, 0, "No chunks were read.")
        print(f"Read {chunk_count} chunks from the large log file.")

    def test_filter_and_structure_logs(self):
        """
        Test filtering and structuring logs into dictionaries.
        """
        sample_log_lines = [
            "2024-12-19 12:00:00 INFO This is an info log",
            "2024-12-19 12:00:01 ERROR This is an error log",
            "2024-12-19 12:00:02 WARNING This is a warning log"
        ]
        structured_logs = filter_and_structure_logs(sample_log_lines)
        self.assertEqual(len(structured_logs), 3, "Number of structured logs should be 3.")
        self.assertIn("timestamp", structured_logs[0], "Structured log entry should contain 'timestamp'.")
        self.assertIn("log_level", structured_logs[0], "Structured log entry should contain 'log_level'.")
        self.assertIn("message", structured_logs[0], "Structured log entry should contain 'message'.")
        print("Log filtering and structuring passed.")

    def test_clean_logs(self):
        """
        Test cleaning of irrelevant logs based on log level and keywords.
        """
        logs = [
            {"timestamp": "2024-12-19 12:00:00", "log_level": "INFO", "message": "This is an info log"},
            {"timestamp": "2024-12-19 12:00:01", "log_level": "ERROR", "message": "This is an error log"},
            {"timestamp": "2024-12-19 12:00:02", "log_level": "WARNING", "message": "This is a warning log"},
            {"timestamp": "2024-12-19 12:00:03", "log_level": "DEBUG", "message": "This is a debug log"}
        ]
        cleaned_logs = clean_logs(logs, exclude_keywords=["debug"], exclude_log_levels=["INFO"])
        self.assertEqual(len(cleaned_logs), 2, "Number of cleaned logs should be 2.")
        self.assertNotIn("DEBUG", [log["log_level"] for log in cleaned_logs], "DEBUG log level should be excluded.")
        self.assertNotIn("INFO", [log["log_level"] for log in cleaned_logs], "INFO log level should be excluded.")
        print("Log cleaning passed.")

    def test_process_large_log_file(self):
        """
        Test the ability to process a large log file, clean, and structure it.
        """
        df = process_large_log_file(
            self.log_file,
            chunk_size=1000,
            exclude_keywords=self.exclude_keywords,
            exclude_log_levels=self.exclude_log_levels
        )
        self.assertGreater(len(df), 0, "The processed DataFrame should have rows.")
        self.assertTrue("timestamp" in df.columns, "'timestamp' column is missing in DataFrame.")
        self.assertTrue("log_level" in df.columns, "'log_level' column is missing in DataFrame.")
        self.assertTrue("message" in df.columns, "'message' column is missing in DataFrame.")
        print(f"Processed large log file into DataFrame with {len(df)} entries.")

    def test_save_dataframe_to_csv(self):
        """
        Test saving the structured DataFrame to a CSV file.
        """
        df = pd.DataFrame({
            "timestamp": ["2024-12-19 12:00:00", "2024-12-19 12:00:01"],
            "log_level": ["ERROR", "INFO"],
            "message": ["Test error log", "Test info log"]
        })
        output_file = "test_output.csv"
        save_dataframe_to_csv(df, output_file)
        self.assertTrue(os.path.exists(output_file), "CSV file should be saved.")
        saved_df = pd.read_csv(output_file)
        self.assertEqual(len(saved_df), 2, "The number of entries in the saved CSV should match.")
        os.remove(output_file)
        print(f"DataFrame saved to {output_file}.")

if __name__ == "__main__":
    unittest.main()
