from flask import Flask, jsonify, request
import pandas as pd
import os

app = Flask(__name__)

PROCESSED_LOGS_PATH = "../processed_logs.csv"


@app.route("/api/processed_logs", methods=["GET"])
def get_processed_logs():
    """
    API endpoint to fetch processed logs from the processed_logs.csv file.
    """
    if not os.path.exists(PROCESSED_LOGS_PATH):
        return jsonify({"error": f"File '{PROCESSED_LOGS_PATH}' not found!"}), 404

    try:
        df = pd.read_csv(PROCESSED_LOGS_PATH)

        level = request.args.get("level")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        if level:
            df = df[df["level"] == level]

        if start_date:
            df = df[df["timestamp"] >= start_date]

        if end_date:
            df = df[df["timestamp"] <= end_date]

        logs_json = df.to_dict(orient="records")
        return jsonify(logs_json)

    except Exception as e:
        return jsonify({"error": f"Error reading the file: {str(e)}"}), 500


@app.route("/processed_logs", methods=["GET"])
def display_processed_logs():
    """
    Web route to display processed logs in an HTML table.
    """
    if not os.path.exists(PROCESSED_LOGS_PATH):
        return "<h1>Error: Processed logs file not found!</h1>"

    try:
        df = pd.read_csv(PROCESSED_LOGS_PATH)

        html_table = df.to_html(classes="table table-striped", index=False)

        return f"""
        <html>
        <head>
            <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
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
        return f"<h1>Error loading logs: {str(e)}</h1>"


if __name__ == "__main__":
    app.run(debug=True)
