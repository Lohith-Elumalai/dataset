import openai
import pandas as pd

# Set up your OpenAI API key
openai.api_key = "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"

def parse_logs(log_file):
    """
    Parse the log file to extract relevant data.
    Assume logs are in CSV format with columns: Timestamp, Component, Severity, Message.
    """
    logs = pd.read_csv(log_file)
    return logs

def generate_root_cause_prompt(logs):
    """
    Create a structured prompt to analyze the logs and suggest root causes.
    """
    # Summarize the logs
    summary = logs.groupby(["Component", "Severity"]).size().reset_index(name="Count")
    summary_text = summary.to_string(index=False)

    prompt = (
        f"Below are logs from a network outage investigation. Analyze them and provide "
        f"the likely root cause. If possible, suggest specific issues or components to investigate:\n\n"
        f"{summary_text}\n\n"
        f"Full Logs:\n{logs[['Timestamp', 'Component', 'Message']].head(10).to_string(index=False)}\n\n"
        f"Provide a detailed analysis and suggest root causes."
    )
    return prompt

def find_root_cause(log_file):
    """
    Main function to analyze logs and identify root causes using OpenAI.
    """
    logs = parse_logs(log_file)
    prompt = generate_root_cause_prompt(logs)

    # Query OpenAI for analysis
    response = openai.Completion.create(
        engine="text-davinci-003",  # Use a suitable LLM model
        prompt=prompt,
        max_tokens=500,
        temperature=0.7,
    )
    return response.choices[0].text.strip()

# Example usage
log_file = "network_logs.csv"  # Replace with your log file path
root_cause_analysis = find_root_cause(log_file)
print("Root Cause Analysis:")
print(root_cause_analysis)
