import os
import logging
import json
from typing import List
from datetime import datetime
from collections import defaultdict
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class LogParser:
    def __init__(self, file_path: str, batch_size: int = 10000):
        """
        Initializes the LogParser with the log file path and batch size.

        Args:
            file_path (str): Path to the large log file.
            batch_size (int): The number of log entries to process at once (default is 10,000).
        """
        self.file_path = file_path
        self.batch_size = batch_size
        self.total_logs = 0
        self.filtered_logs = []

    def parse_logs(self):
        """
        Parses the log file in batches, processes each batch, and stores relevant logs.

        This method reads logs line by line to avoid memory overload with large files.
        """
        logger.info(f"Parsing log file: {self.file_path}")
        
        try:
            with open(self.file_path, 'r') as log_file:
                batch = []
                for line in log_file:
                    batch.append(line.strip())

                    # Process batch when it's full
                    if len(batch) >= self.batch_size:
                        self.process_batch(batch)
                        batch = []  # Reset batch for the next group of lines
                
                # Process any remaining lines after the loop
                if batch:
                    self.process_batch(batch)
            
            # After parsing, export or handle filtered logs
            self.export_filtered_logs()

        except Exception as e:
            logger.error(f"Error while parsing logs: {e}")

    def process_batch(self, batch: List[str]):
        """
        Processes each batch of log entries. It filters and parses the logs into a structured format.

        Args:
            batch (List[str]): A list of raw log entries to process.
        """
        logger.info(f"Processing a batch of {len(batch)} logs.")

        for log_entry in batch:
            try:
                parsed_log = self.parse_log_entry(log_entry)
                if parsed_log:
                    self.filtered_logs.append(parsed_log)
                    self.total_logs += 1
            except Exception as e:
                logger.error(f"Error processing log entry: {log_entry}. Error: {e}")

    def parse_log_entry(self, log_entry: str) -> dict:
        """
        Parses a single log entry. Here, you can adapt this method depending on your log structure.

        Args:
            log_entry (str): A single log entry in text form.

        Returns:
            dict: A parsed log entry or None if the entry is irrelevant.
        """
        try:
            # Example parsing logic: Let's assume the log is in JSON format or follows a certain structure.
            log_data = json.loads(log_entry)  # For JSON logs

            # Example: A log entry might have 'timestamp', 'level', and 'message'
            timestamp = log_data.get("timestamp")
            level = log_data.get("level")
            message = log_data.get("message")

            # Filtering: Only keep logs with 'ERROR' or 'WARNING' level
            if level in ['ERROR', 'WARNING']:
                return {
                    "timestamp": timestamp,
                    "level": level,
                    "message": message
                }
            else:
                return None

        except Exception as e:
            logger.warning(f"Failed to parse log entry: {log_entry}. Error: {e}")
            return None

    def export_filtered_logs(self):
        """
        Export the filtered logs into a CSV or other formats for further analysis.

        This method will convert the filtered logs to a pandas DataFrame and export it as a CSV.
        """
        if self.filtered_logs:
            # Convert filtered logs to a DataFrame
            df = pd.DataFrame(self.filtered_logs)

            # Save to CSV
            output_file = "filtered_logs.csv"
            df.to_csv(output_file, index=False)
            logger.info(f"Exported {len(self.filtered_logs)} filtered logs to {output_file}")
        else:
            logger.info("No filtered logs to export.")

    def get_stats(self):
        """
        Get statistics about the parsing process (e.g., total logs processed).

        Returns:
            dict: A dictionary with statistics on logs.
        """
        return {
            "total_logs_processed": self.total_logs,
            "filtered_logs": len(self.filtered_logs)
        }

# Example of usage
if __name__ == "__main__":
    # Set the log file path (for example, 'large_log_file.txt')
    log_file_path = "large_log_file.txt"  # Update with actual path to your log file

    # Create LogParser instance
    parser = LogParser(file_path=log_file_path, batch_size=10000)

    # Parse the logs
    parser.parse_logs()

    # Get and print stats about the parsed logs
    stats = parser.get_stats()
    logger.info(f"Parsing complete. Stats: {stats}")
