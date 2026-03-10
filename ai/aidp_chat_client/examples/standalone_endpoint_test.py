"""
Standalone Endpoint Test Script - Complete working example without library installation.

This is a self-contained script that can run independently.
It includes all necessary functions inline.
"""

import os
import uuid
from typing import Any, Dict, List, Optional
import requests
from typing import List, Dict, Any
import time
import random
import oci
import json
import os
import re


class AIDPChatClient:
    def __init__(self, aidp_url: str, config, auth):
        self.aidp_url = aidp_url.rstrip("/")
        if (auth == "security_token"):
            with open(config["security_token_file"], 'r') as f:
                token = f.read()
            private_key = oci.signer.load_private_key_from_file(config["key_file"])
            self.signer = oci.auth.signers.SecurityTokenSigner(token, private_key)
        else:
            self.signer = oci.Signer(
                tenancy=config['tenancy'],
                user=config['user'],
                fingerprint=config['fingerprint'],
                private_key_file_location=config['key_file'],
            )

    def _post(self, path: str, body=None, params=None, headers={"Content-Type": "application/json"}):
        url = f"{self.aidp_url}{path}"
        resp = requests.post(url, params=params, auth=self.signer, json=body, headers=headers)
        resp.raise_for_status()
        return resp

    def _post_binary(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.post(url, params=params, auth=self.signer, data=body, headers={"Content-Type": "application/octet-stream"})
        resp.raise_for_status()
        return resp

    def _put(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.put(url, params=params, auth=self.signer, json=body, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp

    def _patch(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.patch(url, params=params, auth=self.signer, json=body, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp

    def _get(self, path: str, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.get(url, params=params, auth=self.signer, headers={"Content-Type": "application/json"})
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str, obj=None):
        url = f"{self.aidp_url}{path}/{obj}"
        resp = requests.delete(url, auth=self.signer)
        resp.raise_for_status()
        return resp

    def create_session(self, body):
        """Create session."""
        return self._post("/session", body)

    def post(self, body):
        """Create post."""
        return self._post("", body)


def chat(client: AIDPChatClient, input_text: str, last_response_id: str):
    response = client.post({
        "isStreaEnabled": False,
        "input": [{
            "role": "user",
            "content": [{
                "type": "INPUT_TEXT",
                "text": input_text
            }]
        }],
        "previous_response_id": last_response_id
    })
    return response


def chat_stream(client: AIDPChatClient, input_text: str, last_response_id: str):
    """Stream chat response and return accumulated data."""
    accumulated_text = ""
    response_data = None
    
    with client.post({
        "stream": True,
        "input": [{
            "role": "user",
            "content": [{
                "type": "INPUT_TEXT",
                "text": input_text
            }]
        }],
        "previous_response_id": last_response_id
    }) as response:
        response.raise_for_status()
        
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                if decoded_line.startswith("data:"):
                    event_data = decoded_line[len("data:"):].strip()
                    try:
                        # Parse the JSON event data
                        event_json = json.loads(event_data)
                        
                        # Store the most recent complete response
                        if event_json.get("type") == "response.created":
                            response_data = event_json.get("response", {})
                        
                        # Accumulate text content
                        outputs = event_json.get("response", {}).get("output", [])
                        for output in outputs:
                            contents = output.get("content", [])
                            for content in contents:
                                if isinstance(content, dict) and "text" in content:
                                    accumulated_text = content.get("text", "")
                                    
                    except json.JSONDecodeError:
                        print(f"Warning: Could not parse event data: {event_data[:100]}...")
                        
                elif decoded_line.startswith("event:"):
                    event_type = decoded_line[len("event:"):].strip()
                    # Optional: handle specific event types
                    
    # Return the accumulated response data
    return response_data if response_data else {"output": [{"content": [{"text": accumulated_text}]}]}


def get_last_response_id(response):
    """Extract response ID from response."""
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        else:
            # Try to parse as JSON
            response_json = response.json()
        
        response_id = response_json.get("id")
        return response_id
    except Exception as e:
        print(f"Error getting response ID: {e}")
        return None


def print_response_content(response):
    """Print response content with error handling."""
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        elif isinstance(response, str):
            # If response is a string, try to parse as JSON
            response_json = json.loads(response)
        else:
            # Try to get JSON from response object
            response_json = response.json()
        
        # Print raw response for debugging
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


def get_traces(response):
    """Extract traces from response."""
    try:
        # Check if response is already a dict
        if isinstance(response, dict):
            response_json = response
        else:
            response_json = response.json()
        
        outputs = response_json.get("output", [])
        for aoutput in outputs:
            txts = aoutput.get("content", [])
            for txt in txts:
                return txt.get("trace")
    except Exception as e:
        print(f"Error getting traces: {e}")
        return None


if __name__ == "__main__":
    # ========================================
    # CONFIGURATION - Update these values
    # ========================================
    
    # Your AIDP agent endpoint URL
    url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/13e012d7002943d9969587a237564d8d/chat"
    
    # Load OCI config
    config = oci.config.from_file("/Workspace/oci_config.ini")
    auth = ""  # "" or "api_key" or "security_token"
    
    # ========================================
    # TEST EXECUTION
    # ========================================
    
    # Create client
    client = AIDPChatClient(url, config, auth)
    
    print("=" * 60)
    print("Testing AIDP Chat - Standalone Version")
    print("=" * 60)
    
    # Example 1: Using streaming chat (recommended for real-time responses)
    print("\n### Example 1: Streaming Chat ###")
    response = chat_stream(client, "Tell me about plants", None)
    print_response_content(response)
    
    # Get response ID for follow-up
    response_id = get_last_response_id(response)
    
    # Example 2: Follow-up question with context
    if response_id:
        print("\n### Example 2: Follow-up Question ###")
        followup = chat_stream(client, "Tell me more about their benefits", response_id)
        print_response_content(followup)
    
    # Example 3: Non-streaming chat (simpler but slower to start)
    print("\n### Example 3: Non-Streaming Chat ###")
    simple_response = chat(client, "What can you help me with?", None)
    print_response_content(simple_response)
    
    print("\n" + "=" * 60)
    print("✅ Testing complete!")
    print("=" * 60)
