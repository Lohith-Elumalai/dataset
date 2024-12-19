import re
from datetime import datetime


def parse_log(log_entry):
    """
    Extract relevant information from a generic log entry.

    Supported fields:
    - Timestamp (if present)
    - Severity level (INFO, WARNING, ERROR)
    - Message content
    - IP addresses (if present)
    - Error codes (if present)

    Returns:
        dict: Parsed log details.
    """
    # General log regex with optional timestamp
    log_pattern = re.compile(
        r"(?:(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s)?(ERROR|WARNING|INFO):\s(.+)"
    )

    match = log_pattern.match(log_entry)
    if match:
        timestamp, level, message = match.groups()
        log_details = {
            "timestamp": timestamp or "N/A",
            "level": level,
            "message": message.strip(),
            "ip_addresses": extract_ip_addresses(message),
            "error_codes": extract_error_codes(message),
        }
        return log_details

    # Fallback for logs that don't match the main pattern
    return {
        "timestamp": "N/A",
        "level": "UNKNOWN",
        "message": log_entry.strip(),
        "ip_addresses": extract_ip_addresses(log_entry),
        "error_codes": extract_error_codes(log_entry),
    }


def extract_ip_addresses(text):
    """
    Extract all IP addresses from the given text.

    Returns:
        list: A list of extracted IP addresses.
    """
    ip_pattern = re.compile(
        r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
    )  # Matches IPv4 addresses
    return ip_pattern.findall(text)


def extract_error_codes(text):
    """
    Extract error codes from the given text.
    Commonly follows patterns like 'E123', 'ERR_456', etc.

    Returns:
        list: A list of extracted error codes.
    """
    error_code_pattern = re.compile(r"\b(E\d{3,4}|ERR_\d+)\b")
    return error_code_pattern.findall(text)


def parse_timestamp(timestamp):
    """
    Parse a timestamp string into a datetime object.

    Args:
        timestamp (str): Timestamp string in 'YYYY-MM-DD HH:MM:SS' format.

    Returns:
        datetime: Parsed datetime object, or None if parsing fails.
    """
    try:
        return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def parse_batch_logs(log_entries):
    """
    Parse a batch of log entries.

    Args:
        log_entries (list): List of log entries.

    Returns:
        list: List of parsed log details.
    """
    return [parse_log(log_entry) for log_entry in log_entries]


# Example Usage
if __name__ == "__main__":
    sample_logs = [
        "2024-12-19 14:22:31 ERROR: Packet loss detected between Node A and Node B.",
        "2024-12-19 14:25:12 WARNING: High latency observed in region X.",
        "INFO: Network maintenance scheduled.",
        "ERROR: Connection timeout (IP: 192.168.1.1, Error Code: ERR_503).",
        "Invalid log entry without format."
    ]

    parsed_logs = parse_batch_logs(sample_logs)

    for i, parsed_log in enumerate(parsed_logs):
        print(f"Log {i+1}:\n{parsed_log}\n")
