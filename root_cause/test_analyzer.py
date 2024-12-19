import unittest
from unittest.mock import patch, MagicMock

import openai
from analyzer import analyze_log_entry

class TestAnalyzer(unittest.TestCase):
    
    @patch('openai.Completion.create')
    def test_analyze_log_entry_success(self, mock_create):
        """
        Test successful root cause analysis of a log entry.
        """
        # Mock the OpenAI response
        mock_response = MagicMock()
        mock_response['choices'][0]['text'] = "The issue is caused by a network link failure between Node A and Node B."
        mock_create.return_value = mock_response
        
        log_entry = "ERROR: Packet loss detected between Node A and Node B."
        
        # Call the function
        result = analyze_log_entry(log_entry)
        
        # Assertions
        self.assertEqual(result, "The issue is caused by a network link failure between Node A and Node B.")
        mock_create.assert_called_once_with(
            engine="gpt-4",
            prompt=("The following is a network log entry. Analyze the log entry "
                    "to identify the root cause of the issue:\n\nERROR: Packet loss detected between Node A and Node B.\n\n"
                    "Provide a concise and clear explanation of the root cause."),
            max_tokens=150,
            temperature=0.3,
            top_p=1,
            n=1,
            stop=None
        )

    @patch('openai.Completion.create')
    def test_analyze_log_entry_invalid(self, mock_create):
        """
        Test when the log entry is invalid (empty string or None).
        """
        log_entry = ""
        result = analyze_log_entry(log_entry)
        self.assertEqual(result, "Error: Invalid log entry.")
        
        log_entry = None
        result = analyze_log_entry(log_entry)
        self.assertEqual(result, "Error: Invalid log entry.")
        
        # Ensure OpenAI API is not called when the log entry is invalid
        mock_create.assert_not_called()

    @patch('openai.Completion.create')
    def test_analyze_log_entry_openai_error(self, mock_create):
        """
        Test OpenAI API error handling.
        """
        # Simulate an OpenAI API error
        mock_create.side_effect = openai.error.OpenAIError("API request failed")
        
        log_entry = "ERROR: Authentication failed for user admin on Node C."
        result = analyze_log_entry(log_entry)
        
        # Assert that the error handling works as expected
        self.assertEqual(result, "Error: Failed to analyze due to an OpenAI API error.")
        mock_create.assert_called_once()

    @patch('openai.Completion.create')
    def test_analyze_log_entry_unexpected_error(self, mock_create):
        """
        Test handling of unexpected errors during analysis.
        """
        # Simulate a generic exception
        mock_create.side_effect = Exception("Unexpected error")
        
        log_entry = "ERROR: Node B is unreachable."
        result = analyze_log_entry(log_entry)
        
        # Assert that the unexpected error is handled
        self.assertEqual(result, "Error: Unexpected failure during analysis.")
        mock_create.assert_called_once()

if __name__ == "__main__":
    unittest.main()
