"""
Utility functions for working with AIDP Chat responses.
"""

import json
from typing import Any, Dict, Optional, Union
import requests


def get_last_response_id(response: Union[requests.Response, Dict[str, Any], str]) -> Optional[str]:
    """
    Extract response ID from a chat response.
    
    Args:
        response: The response object, dictionary, or JSON string
        
    Returns:
        The response ID or None if not found
    """
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        elif isinstance(response, str):
            response_json = json.loads(response)
        else:
            # Try to parse as JSON from response object
            response_json = response.json()
        
        response_id = response_json.get("id")
        return response_id
    except Exception as e:
        print(f"Error getting response ID: {e}")
        return None


def print_response_content(response: Union[requests.Response, Dict[str, Any], str], verbose: bool = False) -> None:
    """
    Print the content from a chat response with error handling.
    
    Args:
        response: The response object, dictionary, or JSON string
        verbose: Whether to print the raw JSON response
    """
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        elif isinstance(response, str):
            response_json = json.loads(response)
        else:
            response_json = response.json()
        
        # Print raw response for debugging if verbose
        if verbose:
            print("\n--- Raw Response ---")
            print(json.dumps(response_json, indent=2))
            print("--- End Raw Response ---\n")
        
        # Extract and print content
        outputs = response_json.get("output", [])
        if not outputs:
            print("No output found in response")
            return
        
        print("\n--- Agent Response ---")
        for aoutput in outputs:
            txts = aoutput.get("content", [])
            for txt in txts:
                if isinstance(txt, dict):
                    print(txt.get("text", ""))
                else:
                    print(txt)
        print("--- End Agent Response ---\n")
    
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        print(f"Raw response: {response}")
    except AttributeError as e:
        print(f"Error accessing response attributes: {e}")
        print(f"Response type: {type(response)}")
        print(f"Response: {response}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Response type: {type(response)}")
        print(f"Response: {response}")


def get_traces(response: Union[requests.Response, Dict[str, Any], str]) -> Optional[Any]:
    """
    Extract trace information from a chat response.
    
    Args:
        response: The response object, dictionary, or JSON string
        
    Returns:
        The trace information or None if not found
    """
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        elif isinstance(response, str):
            response_json = json.loads(response)
        else:
            response_json = response.json()
        
        outputs = response_json.get("output", [])
        for aoutput in outputs:
            txts = aoutput.get("content", [])
            for txt in txts:
                if isinstance(txt, dict) and "trace" in txt:
                    return txt.get("trace")
        return None
    except Exception as e:
        print(f"Error getting traces: {e}")
        return None


def extract_text_content(response: Union[requests.Response, Dict[str, Any], str]) -> str:
    """
    Extract plain text content from a chat response.
    
    Args:
        response: The response object, dictionary, or JSON string
        
    Returns:
        The extracted text content
    """
    try:
        if isinstance(response, dict):
            response_json = response
        elif isinstance(response, str):
            response_json = json.loads(response)
        else:
            response_json = response.json()
        
        text_parts = []
        outputs = response_json.get("output", [])
        for aoutput in outputs:
            contents = aoutput.get("content", [])
            for content in contents:
                if isinstance(content, dict) and "text" in content:
                    text_parts.append(content["text"])
                elif isinstance(content, str):
                    text_parts.append(content)
        
        return "\n".join(text_parts)
    except Exception as e:
        print(f"Error extracting text content: {e}")
        return ""
