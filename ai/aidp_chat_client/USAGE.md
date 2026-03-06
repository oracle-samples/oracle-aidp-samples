# AIDP Chat Client - Usage Guide

Complete guide for using the AIDP Chat Client library.

## Table of Contents

1. [Installation](#installation)
2. [Authentication Setup](#authentication-setup)
3. [Basic Usage](#basic-usage)
4. [Advanced Features](#advanced-features)
5. [Best Practices](#best-practices)
6. [Troubleshooting](#troubleshooting)

## Installation

### Method 1: Install from Source

```bash
cd aidp_chat_client
pip install -e .
```

### Method 2: Install Requirements Only

```bash
pip install -r aidp_chat_client/requirements.txt
```

Then import directly in your Python code:
```python
import sys
sys.path.append('/path/to/aidp_chat_client')
from client import AIDPChatClient
```

## Authentication Setup

### Option 1: API Key Authentication

1. Create an OCI config file (e.g., `~/.oci/config`):

```ini
[DEFAULT]
user=ocid1.user.oc1..aaaaaaaxxxxx
fingerprint=aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
tenancy=ocid1.tenancy.oc1..aaaaaaaxxxxx
region=us-ashburn-1
key_file=/path/to/your/private_key.pem
```

2. Use in code:

```python
from aidp_chat_client.config import load_oci_config

config = load_oci_config("~/.oci/config")
client = AIDPChatClient(aidp_url, config, auth="api_key")
```

### Option 2: Security Token Authentication

1. Generate a security token and save it to a file

2. Create config dictionary:

```python
config = {
    'security_token_file': '/path/to/token',
    'key_file': '/path/to/private_key.pem'
}

client = AIDPChatClient(aidp_url, config, auth="security_token")
```

## Basic Usage

### Simple Chat

```python
from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import print_response_content

# Setup
aidp_url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"
config = load_oci_config("/path/to/config.ini")
client = AIDPChatClient(aidp_url, config)

# Send message
response = client.chat("Hello, how are you?")
print_response_content(response)
```

### Streaming Chat

```python
# Enable streaming for real-time responses
response = client.chat("Tell me a long story", stream=True)
print_response_content(response)
```

### Conversation with Context

```python
from aidp_chat_client.utils import get_last_response_id

# First message
response1 = client.chat("What is Python?")
print_response_content(response1)

# Follow-up with context
response_id = get_last_response_id(response1)
response2 = client.chat("What are its main features?", previous_response_id=response_id)
print_response_content(response2)
```

## Advanced Features

### Custom Request Headers

```python
# Access low-level methods for custom requests
response = client._post(
    "/custom-endpoint",
    body={"key": "value"},
    headers={"Custom-Header": "value"}
)
```

### Binary Data Upload

```python
# Upload binary data
with open("document.pdf", "rb") as f:
    binary_data = f.read()

response = client._post_binary("/upload", body=binary_data)
```

### Session Management

```python
# Create a custom session
session_config = {
    "session_id": "my-session-123",
    "timeout": 3600
}

session_response = client.create_session(session_config)
```

### Extract Specific Content

```python
from aidp_chat_client.utils import extract_text_content, get_traces

# Get plain text
response = client.chat("Explain quantum computing")
text = extract_text_content(response)
print(text)

# Get trace information (for debugging)
traces = get_traces(response)
if traces:
    print("Trace info:", traces)
```

### Verbose Output

```python
# Enable verbose mode to see raw JSON
response = client.chat("Hello")
print_response_content(response, verbose=True)
```

## Best Practices

### 1. Error Handling

Always wrap API calls in try-except blocks:

```python
import requests

try:
    response = client.chat("Hello")
    print_response_content(response)
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
    print(f"Status Code: {e.response.status_code}")
except requests.exceptions.ConnectionError:
    print("Connection error - check your network")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### 2. Rate Limiting

Add delays between requests to avoid rate limiting:

```python
import time

queries = ["Query 1", "Query 2", "Query 3"]
for query in queries:
    response = client.chat(query)
    print_response_content(response)
    time.sleep(1)  # 1 second delay
```

### 3. Configuration Validation

Validate configuration before creating client:

```python
from aidp_chat_client.config import validate_config

try:
    validate_config(config, auth_type="api_key")
    client = AIDPChatClient(aidp_url, config)
except ValueError as e:
    print(f"Configuration error: {e}")
    exit(1)
```

### 4. Resource Cleanup

Use context managers when appropriate:

```python
class ChatSession:
    def __init__(self, client):
        self.client = client
        self.response_id = None
    
    def chat(self, message):
        response = self.client.chat(message, previous_response_id=self.response_id)
        self.response_id = get_last_response_id(response)
        return response

# Use it
with ChatSession(client) as session:
    session.chat("Hello")
    session.chat("How are you?")
```

### 5. Logging

Implement proper logging:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    response = client.chat("Hello")
    logger.info("Chat request successful")
    print_response_content(response)
except Exception as e:
    logger.error(f"Chat request failed: {e}")
```

## Troubleshooting

### Issue: Authentication Errors

**Solution:**
- Verify your OCI config file is correctly formatted
- Check that private key file exists and is readable
- Ensure fingerprint matches your API key
- Verify user and tenancy OCIDs are correct

### Issue: Connection Timeout

**Solution:**
```python
import requests

# Increase timeout
response = client._post(
    "",
    body={"input": [{"role": "user", "content": [{"type": "INPUT_TEXT", "text": "Hello"}]}]},
)
```

### Issue: Streaming Not Working

**Solution:**
- Ensure `stream=True` is set in the chat method
- Check that the endpoint supports streaming
- Verify network/firewall allows streaming connections

### Issue: Response ID Not Found

**Solution:**
```python
response = client.chat("Hello")
response_id = get_last_response_id(response)

if not response_id:
    print("Warning: No response ID found")
    # Handle accordingly - maybe create new conversation
else:
    # Continue with response_id
    next_response = client.chat("Follow-up", previous_response_id=response_id)
```

### Issue: Import Errors

**Solution:**
```bash
# Ensure package is installed
pip install -e /path/to/aidp_chat_client

# Or add to Python path
export PYTHONPATH="${PYTHONPATH}:/path/to/aidp_chat_client"
```

## Support

For additional help:
- Check the [README.md](README.md) for basic information
- Review the [examples/](examples/) directory for working code
- Open an issue on GitHub for bug reports
- Consult Oracle AIDP documentation

## License

Copyright (c) 2024 Oracle and/or its affiliates.
Licensed under the Universal Permissive License (UPL), Version 1.0.
