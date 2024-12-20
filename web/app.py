from flask import Flask, render_template, jsonify
import pandas as pd
import os

app = Flask(__name__)

# File paths
PROCESSED_LOGS_PATH = "processed_logs.csv"
LARGE_LOG_FILE_PATH = "large_log_file.txt"
ROOT_CAUSE_ANALYSIS_PATH = "root_cause_analysis.log"


@app.route("/")
def home():
    """
    Home page displaying links to different sections.
    """
    return """
    <html>
        <head>
            <title>Log Analysis Dashboard</title>
        </head>
        <body>
            <h1>Log Analysis Dashboard</h1>
            <ul>
                <li><a href="/processed_logs">Processed Logs</a></li>
                <li><a href="/errors">Error Logs</a></li>
                <li><a href="/root_cause">Root Cause Analysis</a></li>
            </ul>
        </body>
    </html>
    """


@app.route("/processed_logs")
def processed_logs():
    """
    Display the processed logs from `processed_logs.csv`.
    """
    if not os.path.exists(PROCESSED_LOGS_PATH):
        return "<h1>Error: Processed logs file not found!</h1>"

    try:
        # Load the CSV file
        df = pd.read_csv(PROCESSED_LOGS_PATH)

        # Convert to an HTML table
        html_table = df.to_html(classes="table table-striped", index=False)

        return f"""
        <html>
        <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
            <title>Processed Logs</title>
        </head>
        <body>
            <div class="container">
                <h1>Processed Logs</h1>
                {html_table}
            </div>
        </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error loading processed logs: {str(e)}</h1>"


@app.route("/errors")
def errors():
    """
    Display error logs from `large_log_file.txt`.
    """
    if not os.path.exists(LARGE_LOG_FILE_PATH):
        return "<h1>Error: Large log file not found!</h1>"

    try:
        # Read the file contents
        with open(LARGE_LOG_FILE_PATH, "r") as file:
            logs = file.readlines()

        # Format the logs for display
        logs_html = "<br>".join(logs)

        return f"""
        <html>
        <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
            <title>Error Logs</title>
        </head>
        <body>
            <div class="container">
                <h1>Error Logs</h1>
                <pre>{logs_html}</pre>
            </div>
        </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error reading error logs: {str(e)}</h1>"


@app.route("/root_cause")
def root_cause():
    """
    Display the root cause analysis from `root_cause_analysis.log`.
    """
    if not os.path.exists(ROOT_CAUSE_ANALYSIS_PATH):
        return "<h1>Error: Root cause analysis file not found!</h1>"

    try:
        # Read the file contents
        with open(ROOT_CAUSE_ANALYSIS_PATH, "r") as file:
            root_cause = file.readlines()

        # Format the root cause analysis for display
        root_cause_html = "<br>".join(root_cause)

        return f"""
        <html>
        <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
            <title>Root Cause Analysis</title>
        </head>
        <body>
            <div class="container">
                <h1>Root Cause Analysis</h1>
                <pre>{root_cause_html}</pre>
            </div>
        </body>
        </html>
        """
    except Exception as e:
        return f"<h1>Error reading root cause analysis: {str(e)}</h1>"


if __name__ == "__main__":
    app.run(debug=True)
