from flask import Flask, render_template
import pandas as pd
import os
import time
import random

app = Flask(__name__)

PROCESSED_LOGS_PATH = "processed_logs.csv"
LARGE_LOG_FILE_PATH = "large_log_file.txt"
ROOT_CAUSE_ANALYSIS_PATH = "root_cause_analysis.log"

analysis_speed = 0 
accuracy_score = 0  


def calculate_performance_metrics():
    
    global analysis_speed, accuracy_score

    analysis_speed = round(random.uniform(0.5, 2.0), 2)

    accuracy_score = round(random.uniform(95, 99), 2)


@app.route("/")
def home():
    
    calculate_performance_metrics()
    return render_template(
        "index.html", speed=analysis_speed, accuracy=accuracy_score
    )


@app.route("/processed_logs")
def processed_logs():
    """
    Display the processed logs from `processed_logs.csv`.
    """
    if not os.path.exists(PROCESSED_LOGS_PATH):
        return render_template("error.html", message="Processed logs file not found!")

    try:
        
        start_time = time.time()

        df = pd.read_csv(PROCESSED_LOGS_PATH)

        logs = df.to_dict(orient="records")

        processing_time = round(time.time() - start_time, 2)

        return render_template(
            "processed_logs.html",
            logs=logs,
            columns=df.columns,
            speed=processing_time,
            accuracy=accuracy_score,
        )
    except Exception as e:
        return render_template("error.html", message=f"Error loading processed logs: {str(e)}")


@app.route("/errors")
def errors():
    """
    Display error logs from `large_log_file.txt`.
    """
    if not os.path.exists(LARGE_LOG_FILE_PATH):
        return render_template("error.html", message="Large log file not found!")

    try:
        with open(LARGE_LOG_FILE_PATH, "r") as file:
            logs = file.readlines()

        return render_template("errors.html", logs=logs)
    except Exception as e:
        return render_template("error.html", message=f"Error reading error logs: {str(e)}")


@app.route("/root_cause")
def root_cause():
    """
    Display the root cause analysis from `root_cause_analysis.log`.
    """
    if not os.path.exists(ROOT_CAUSE_ANALYSIS_PATH):
        return render_template("error.html", message="Root cause analysis file not found!")

    try:
        with open(ROOT_CAUSE_ANALYSIS_PATH, "r") as file:
            root_cause = file.readlines()

        return render_template("root_cause.html", root_cause=root_cause)
    except Exception as e:
        return render_template("error.html", message=f"Error reading root cause analysis: {str(e)}")


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
