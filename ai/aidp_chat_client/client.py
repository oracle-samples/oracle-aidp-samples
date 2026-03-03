"""
AIDP Chat Client module for interacting with Oracle AIDP Chat Agent endpoints.
"""

import json
from typing import Any, Dict, List, Optional, Union
import requests
import oci


class AIDPChatClient:
    """
    A client for interacting with Oracle AIDP Chat Agent endpoints.
    
    Supports both API key and security token authentication methods.
    
    Attributes:
        aidp_url (str): The base URL for the AIDP agent endpoint
        signer: The OCI signer for request authentication
    """
    
    def __init__(self, aidp_url: str, config: Dict[str, Any], auth: str = "api_key"):
        """
        Initialize the AIDP Chat Client.
        
        Args:
            aidp_url: The AIDP agent endpoint URL
            config: OCI configuration dictionary
            auth: Authentication method ("api_key" or "security_token")
        """
        self.aidp_url = aidp_url.rstrip("/")
        
        if auth == "security_token":
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
    
    def _post(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None, 
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """
        Make a POST request to the AIDP endpoint.
        
        Args:
            path: API endpoint path
            body: Request body as dictionary
            params: Query parameters
            headers: Additional headers
            
        Returns:
            Response object
        """
        if headers is None:
            headers = {"Content-Type": "application/json"}
        
        url = f"{self.aidp_url}{path}"
        resp = requests.post(url, params=params, auth=self.signer, json=body, headers=headers)
        resp.raise_for_status()
        return resp
    
    def _post_binary(
        self, 
        path: str, 
        body: Optional[bytes] = None, 
        params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a POST request with binary data.
        
        Args:
            path: API endpoint path
            body: Binary data
            params: Query parameters
            
        Returns:
            Response object
        """
        url = f"{self.aidp_url}{path}"
        resp = requests.post(
            url, 
            params=params, 
            auth=self.signer, 
            data=body, 
            headers={"Content-Type": "application/octet-stream"}
        )
        resp.raise_for_status()
        return resp
    
    def _put(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None, 
        params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a PUT request to the AIDP endpoint.
        
        Args:
            path: API endpoint path
            body: Request body as dictionary
            params: Query parameters
            
        Returns:
            Response object
        """
        url = f"{self.aidp_url}{path}"
        resp = requests.put(
            url, 
            params=params, 
            auth=self.signer, 
            json=body, 
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        return resp
    
    def _patch(
        self, 
        path: str, 
        body: Optional[Dict[str, Any]] = None, 
        params: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """
        Make a PATCH request to the AIDP endpoint.
        
        Args:
            path: API endpoint path
            body: Request body as dictionary
            params: Query parameters
            
        Returns:
            Response object
        """
        url = f"{self.aidp_url}{path}"
        resp = requests.patch(
            url, 
            params=params, 
            auth=self.signer, 
            json=body, 
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        return resp
    
    def _get(
        self, 
        path: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make a GET request to the AIDP endpoint.
        
        Args:
            path: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        url = f"{self.aidp_url}{path}"
        resp = requests.get(
            url, 
            params=params, 
            auth=self.signer, 
            headers={"Content-Type": "application/json"}
        )
        resp.raise_for_status()
        return resp.json()
    
    def _delete(
        self, 
        path: str, 
        obj: Optional[str] = None
    ) -> requests.Response:
        """
        Make a DELETE request to the AIDP endpoint.
        
        Args:
            path: API endpoint path
            obj: Object identifier to delete
            
        Returns:
            Response object
        """
        url = f"{self.aidp_url}{path}"
        if obj:
            url = f"{url}/{obj}"
        resp = requests.delete(url, auth=self.signer)
        resp.raise_for_status()
        return resp
    
    def create_session(self, body: Dict[str, Any]) -> requests.Response:
        """
        Create a new chat session.
        
        Args:
            body: Session configuration
            
        Returns:
            Response object
        """
        return self._post("/session", body)
    
    def chat(
        self, 
        input_text: str, 
        previous_response_id: Optional[str] = None,
        stream: bool = False
    ) -> Union[requests.Response, Dict[str, Any]]:
        """
        Send a chat message to the AIDP agent.
        
        Args:
            input_text: The user's input text
            previous_response_id: ID of previous response for context (optional)
            stream: Whether to stream the response
            
        Returns:
            Response object (non-streaming) or response dictionary (streaming)
        """
        body = {
            "stream": stream,
            "input": [{
                "role": "user",
                "content": [{
                    "type": "INPUT_TEXT",
                    "text": input_text
                }]
            }],
            "previous_response_id": previous_response_id
        }
        
        if stream:
            return self._chat_stream(body)
        else:
            return self._post("", body)
    
    def _chat_stream(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal method to handle streaming chat responses.
        
        Args:
            body: Request body
            
        Returns:
            Accumulated response data as dictionary
        """
        accumulated_text = ""
        response_data = None
        
        with self._post("", body) as response:
            response.raise_for_status()
            
            for line in response.iter_lines():
                if line:
                    decoded_line = line.decode('utf-8')
                    if decoded_line.startswith("data:"):
                        event_data = decoded_line[len("data:"):].strip()
                        try:
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
        
        # Return the accumulated response data
        return response_data if response_data else {
            "output": [{
                "content": [{
                    "text": accumulated_text
                }]
            }]
        }
