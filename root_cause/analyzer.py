import openai
import logging

# Configure logging
logging.basicConfig(
    filename="root_cause_analysis.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# OpenAI API Key
openai.api_key = "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"

def analyze_log_entry(log_entry: str) -> str:
    """
    Analyze the root cause of a single log entry using GPT.
    
    Args:
        log_entry (str): The log entry to analyze.
        
    Returns:
        str: The root cause analysis result.
    """
    try:
        # Ensure the input is valid
        if not log_entry or not isinstance(log_entry, str):
            logging.warning("Invalid log entry provided.")
            return "Error: Invalid log entry."

        logging.info(f"Analyzing log entry: {log_entry}")

        # Create a prompt for the GPT model
        prompt = (
            "The following is a network log entry. Analyze the log entry "
            "to identify the root cause of the issue:\n\n"
            f"{log_entry}\n\n"
            "Provide a concise and clear explanation of the root cause."
        )

        # OpenAI GPT call
        response = openai.Completion.create(
            engine="gpt-4",  # Ensure you're using the correct model engine
            prompt=prompt,
            max_tokens=150,
            temperature=0.3,
            top_p=1,
            n=1,
            stop=None
        )

        # Extract the response text
        root_cause = response['choices'][0]['text'].strip()

        # Log and return the result
        logging.info(f"Analysis completed: {root_cause}")
        return root_cause

    except openai.error.OpenAIError as api_error:
        logging.error(f"OpenAI API error: {api_error}")
        return "Error: Failed to analyze due to an OpenAI API error."

    except Exception as e:
        logging.error(f"Unexpected error during analysis: {e}")
        return "Error: Unexpected failure during analysis."
