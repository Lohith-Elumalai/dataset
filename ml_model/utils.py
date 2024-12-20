import re

def preprocess_log(log):
    """
    Simple text preprocessing for log messages.
    """
    log = log.lower()
    log = re.sub(r'[^\w\s]', '', log)  # Remove punctuation
    log = re.sub(r'\d+', '', log)      # Remove numbers
    return log
