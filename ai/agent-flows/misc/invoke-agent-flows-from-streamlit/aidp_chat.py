import os
import uuid
from typing import Any, Dict, List, Optional
import requests
import time
import random
import oci
import json
import re

class AIDPChatClient:
    def __init__(self, aidp_url: str, config_file: Optional[str] = None, profile: str = "DEFAULT"):
        self.aidp_url = aidp_url.rstrip("/")

        # Detect authentication mode and create appropriate signer
        self.signer = self._create_signer(config_file, profile)

    def _create_signer(self, config_file: Optional[str], profile: str):
        """
        Create an appropriate signer based on the environment:
        - If running in OCI Container Instance, use resource principal
        - If running locally, introspect the OCI profile and use API key or security token
        """
        # Check if running in OCI Container Instance (resource principal environment)
        if os.getenv('OCI_RESOURCE_PRINCIPAL_VERSION'):
            print("Detected OCI Container Instance environment - using resource principal authentication")
            return oci.auth.signers.get_resource_principals_signer()

        # Local mode - load OCI config and introspect profile
        print(f"Using local OCI profile authentication (profile: {profile})")

        try:
            # Load OCI config
            if config_file:
                config = oci.config.from_file(file_location=config_file, profile_name=profile)
            else:
                config = oci.config.from_file(profile_name=profile)

            # Validate config
            #oci.config.validate_config(config)

            # Introspect profile to determine authentication type
            auth_type = self._detect_auth_type(config)

            if auth_type == "security_token":
                print(f"Detected security token authentication in profile '{profile}'")
                return self._create_security_token_signer(config)
            else:
                print(f"Detected API key authentication in profile '{profile}'")
                return self._create_api_key_signer(config)

        except Exception as e:
            raise RuntimeError(f"Failed to create OCI signer: {e}")

    def _detect_auth_type(self, config: Dict[str, Any]) -> str:
        """
        Introspect the OCI config to determine if it uses API key or security token.
        Security token profiles have a 'security_token_file' key.
        """
        if 'security_token_file' in config:
            return "security_token"
        return "api_key"

    def _create_security_token_signer(self, config: Dict[str, Any]):
        """Create a signer for security token authentication."""
        try:
            # Read the security token from file
            with open(config['security_token_file'], 'r') as f:
                token = f.read().strip()

            # Load the private key
            private_key = oci.signer.load_private_key_from_file(config['key_file'])

            # Create and return the security token signer
            return oci.auth.signers.SecurityTokenSigner(token, private_key)
        except Exception as e:
            raise RuntimeError(f"Failed to create security token signer: {e}")

    def _create_api_key_signer(self, config: Dict[str, Any]):
        """Create a signer for API key authentication."""
        try:
            return oci.Signer(
                tenancy=config['tenancy'],
                user=config['user'],
                fingerprint=config['fingerprint'],
                private_key_file_location=config['key_file'],
                pass_phrase=config.get('pass_phrase')  # Optional passphrase for encrypted keys
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create API key signer: {e}")

    def _post(self, path: str, body=None, params=None,headers={"Content-Type":"application/json"}, stream=False):
        url = f"{self.aidp_url}{path}"
        resp = requests.post(url, params=params, auth=self.signer, json=body, headers=headers, stream=stream)
        resp.raise_for_status()
        return resp

    def _post_binary(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.post(url, params=params, auth=self.signer, data=body, headers={"Content-Type":"application/octet-stream"})
        resp.raise_for_status()
        return resp

    def _put(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.put(url, params=params, auth=self.signer, json=body, headers={"Content-Type":"application/json"})
        resp.raise_for_status()
        return resp

    def _patch(self, path: str, body=None, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.patch(url, params=params, auth=self.signer, json=body, headers={"Content-Type":"application/json"})
        resp.raise_for_status()
        return resp

    def _get(self, path: str, params=None):
        url = f"{self.aidp_url}{path}"
        resp = requests.get(url, params=params, auth=self.signer, headers={"Content-Type":"application/json"})
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

    def post(self, body, stream=False):
        """Create post."""
        return self._post("", body, stream=stream)

def chat(client: AIDPChatClient, input_text: str, last_response_id:str):
  response = client.post({"isStreamEnabled":False,"input":[{"role":"user","content":[{"type":"INPUT_TEXT","text": input_text}]}], "previous_response_id":last_response_id})
  return response

class StreamingResponse:
    """Wrapper for streaming chat responses that accumulates data."""

    def __init__(self, client: AIDPChatClient, input_text: str, last_response_id: str):
        self.response = client.post(
            {"isStreamEnabled": True, "input": [{"role": "user", "content": [{"type": "INPUT_TEXT", "text": input_text}]}], "previous_response_id": last_response_id},
            stream=True
        )
        self.accumulated_text = []
        self.response_data = {
            "id": None,
            "output": [],
            "headers": dict(self.response.headers)
        }

    def __iter__(self):
        """Iterate over text chunks from the stream."""
        try:
            for line in self.response.iter_lines():
                if not line:
                    continue

                decoded_line = line.decode('utf-8').strip()

                # Skip empty lines
                if not decoded_line:
                    continue

                #print(f"DEBUG: Raw line: {decoded_line[:200]}")  # DEBUG: Show first 200 chars

                # Try to extract JSON data from various formats
                json_data = None

                # Format 1: SSE format "data: {...}"
                if decoded_line.startswith("data:"):
                    event_data = decoded_line[len("data:"):].strip()

                    # Skip keep-alive messages
                    if not event_data or event_data == "[DONE]":
                        print("DEBUG: Skipping keep-alive or DONE message")
                        continue

                    json_data = event_data

                # Format 2: Plain JSON (JSON-Lines format)
                elif decoded_line.startswith("{"):
                    json_data = decoded_line

                # Format 3: Other SSE events (event:, id:, etc.)
                else:
                 #   print(f"DEBUG: Skipping non-data line: {decoded_line[:100]}")
                    continue

                if json_data:
                    try:
                        chunk_json = json.loads(json_data)
                    #    print(f"DEBUG: Parsed chunk keys: {list(chunk_json.keys())}")

                        # Unwrap the "response" wrapper if present
                        data = chunk_json.get("response", chunk_json)

                        # Capture response ID (check both wrapper and inner response)
                        response_id = chunk_json.get("response", {}).get("id") or chunk_json.get("id")
                        if response_id and not self.response_data["id"]:
                            self.response_data["id"] = response_id
                     #       print(f"DEBUG: Captured response ID: {self.response_data['id']}")

                        # Extract text content from chunk
                        if "output" in data and isinstance(data["output"], list):
                          #  print(f"DEBUG: Found {len(data['output'])} output(s)")
                            for output in data["output"]:
                                if "content" in output and isinstance(output["content"], list):
                                  #  print(f"DEBUG: Found {len(output['content'])} content item(s)")
                                    for content in output["content"]:
                                        # Debug: show content structure
                                        content_type = content.get("type")
                                        has_text = "text" in content
                                        text_preview = content.get("text", "")[:50] if has_text else "N/A"
                                     #   print(f"DEBUG: Content - type={content_type}, has_text={has_text}, preview={text_preview}")

                                        # Skip trace entries
                                        if content_type == "trace":
                                         #   print("DEBUG: Skipping trace content")
                                            continue

                                        # Extract text if present
                                        text = content.get("text", "")
                                        if text:
                                            self.accumulated_text.append(text)
                                        #    print(f"DEBUG: ✓ Yielding text chunk (length={len(text)})")
                                            yield text
                                        else:
                                            print(f"DEBUG: No text in content item")

                        # Capture full output for trace history (usually in final chunk)
                        if "output" in data:
                            self.response_data["output"] = data["output"]

                    except json.JSONDecodeError as e:
                        print(f"DEBUG: JSON decode error: {e}")
                        print(f"DEBUG: Failed to parse: {json_data[:200]}")
                        continue

        finally:
            self.response.close()

    def get_response_data(self):
        """Get the accumulated response data after streaming completes."""
        self.response_data["accumulated_text"] = "".join(self.accumulated_text)
        return self.response_data


def chat_stream(client: AIDPChatClient, input_text: str, last_response_id: str):
    """
    Stream chat responses from AIDP.

    Returns:
        StreamingResponse: An iterable that yields text chunks and provides access to response data

    Example:
        stream = chat_stream(client, "Hello", None)
        for chunk in stream:
            print(chunk, end="")
        response_data = stream.get_response_data()
    """
    return StreamingResponse(client, input_text, last_response_id)

def get_last_response_id(response):
  response_json = response.json()
  response_id = response_json.get("id")
  return response_id

def print_response_content(response):
  response_json = response.json()
  outputs = response_json.get("output")
  for aoutput in outputs:
    txts = aoutput.get("content")
    for txt in txts:
      print(txt.get("text"))

def get_traces(response):
  response_json = response.json()
  outputs = response_json.get("output")
  for aoutput in outputs:
    txts = aoutput.get("content")
    for txt in txts:
      return txt.get("trace")

if __name__ == "__main__":
  # Example usage
  url = "tbd_chat_endpoint"

  # The client will automatically detect the environment:
  # - In OCI Container Instance: uses resource principal
  # - Locally: uses OCI profile (API key or security token)
  client = AIDPChatClient(url, config_file=None, profile="DEFAULT")

  stream = chat_stream(client, "Tell me about the Ryder Cup in 1985 in 3 succinct points", None)

  # Iterate through the stream to process all chunks
  for chunk in stream:
    print(chunk, end="", flush=True)

  print()  # New line after streaming completes

  # Get the full response data after streaming completes
  response_data = stream.get_response_data()
  print(f"\nResponse ID: {response_data['id']}")
