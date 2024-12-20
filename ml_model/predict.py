import pandas as pd
from joblib import load

# Load the trained model and vectorizer
model_path = "./ml_model/model.pkl"
vectorizer_path = "./ml_model/vectorizer.pkl"

model = load(model_path)
vectorizer = load(vectorizer_path)

def predict_category(log_messages):
    """
    Predict categories for a list of log messages.
    """
    # Convert log messages to numeric form using the vectorizer
    log_features = vectorizer.transform(log_messages)

    # Predict using the model
    predictions = model.predict(log_features)
    return predictions

if __name__ == "__main__":
    # Example logs
    example_logs = [
        "Failed to connect to database",
        "Disk usage warning at 90%",
        "User login successful"
    ]
    predictions = predict_category(example_logs)
    for log, category in zip(example_logs, predictions):
        print(f"Log: {log} => Category: {category}")
from joblib import load
import os

model_path = "path/to/your/model/file.pkl"

try:
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model file not found at {model_path}")
    model = load(model_path)
except EOFError:
    raise EOFError(f"Failed to load the model file at {model_path}. File may be corrupted.")
except Exception as e:
    raise RuntimeError(f"An error occurred while loading the model: {str(e)}")
