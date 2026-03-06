# AIDP Chat Client

A Python client library for interacting with Oracle AI Data Platform (AIDP) Chat Agent endpoints.

## 🚀 Quick Start

**Want to test immediately?** Use our standalone script:
```bash
cd examples
python standalone_endpoint_test.py
```
No installation required! Just edit your endpoint URL and config path in the file.

See [QUICKSTART.md](QUICKSTART.md) for detailed quick start guide.

## Features

- 🔐 Support for both API Key and Security Token authentication
- 🌊 Streaming and non-streaming chat responses
- 🛠️ Easy-to-use API with comprehensive error handling
- 📝 Type hints for better IDE support
- 🎯 Utility functions for response parsing and content extraction
- 🔧 Configuration management helpers

## Installation

### From Source

```bash
cd aidp_chat_client
pip install -e .
```

### Install Requirements Only

```bash
pip install -r requirements.txt
```

## Quick Start

### Basic Usage

```python
from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import print_response_content, get_last_response_id

# Your AIDP agent endpoint URL
aidp_url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"

# Load OCI configuration
config = load_oci_config("/path/to/oci_config.ini")

# Create client
client = AIDPChatClient(aidp_url, config, auth="api_key")

# Send a message (non-streaming)
response = client.chat("Tell me about plants", stream=False)
print_response_content(response)
```

### Streaming Response

```python
# Send a message with streaming enabled
response = client.chat("Tell me about plants", stream=True)
print_response_content(response)
```

### Conversation with Context

```python
# First message
response1 = client.chat("Tell me about plants", stream=True)
print_response_content(response1)

# Get response ID for context
response_id = get_last_response_id(response1)

# Follow-up message with context
response2 = client.chat("Tell me more about their benefits", previous_response_id=response_id, stream=True)
print_response_content(response2)
```

### Using Configuration Helper

```python
from aidp_chat_client.config import create_client_from_config

# Create client directly from config file
client = create_client_from_config(
    aidp_url="https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat",
    config_path="/path/to/oci_config.ini",
    profile="DEFAULT",
    auth_type="api_key"
)

response = client.chat("Hello!")
```

## API Reference

### AIDPChatClient

Main client class for interacting with AIDP endpoints.

#### Methods

- `__init__(aidp_url, config, auth="api_key")` - Initialize the client
- `chat(input_text, previous_response_id=None, stream=False)` - Send a chat message
- `create_session(body)` - Create a new chat session
- `_get(path, params=None)` - Make GET request
- `_post(path, body=None, params=None, headers=None)` - Make POST request
- `_put(path, body=None, params=None)` - Make PUT request
- `_patch(path, body=None, params=None)` - Make PATCH request
- `_delete(path, obj=None)` - Make DELETE request

### Utility Functions

- `get_last_response_id(response)` - Extract response ID from response
- `print_response_content(response, verbose=False)` - Pretty print response content
- `get_traces(response)` - Extract trace information from response
- `extract_text_content(response)` - Extract plain text from response

### Configuration Functions

- `load_oci_config(config_path=None, profile="DEFAULT")` - Load OCI configuration
- `validate_config(config, auth_type="api_key")` - Validate configuration
- `create_client_from_config(...)` - Create client from config file

## Authentication

### API Key Authentication

```python
config = {
    'tenancy': 'ocid1.tenancy.oc1...',
    'user': 'ocid1.user.oc1...',
    'fingerprint': 'aa:bb:cc:dd:ee:ff:...',
    'key_file': '/path/to/private_key.pem'
}

client = AIDPChatClient(aidp_url, config, auth="api_key")
```

### Security Token Authentication

```python
config = {
    'security_token_file': '/path/to/token',
    'key_file': '/path/to/private_key.pem'
}

client = AIDPChatClient(aidp_url, config, auth="security_token")
```

## Examples

See the `examples/` directory for more comprehensive examples:

- `basic_chat.py` - Simple chat example
- `streaming_chat.py` - Streaming response example
- `conversation_context.py` - Multi-turn conversation with context
- `batch_processing.py` - Processing multiple queries
- `complete_endpoint_example.py` - Full example matching original notebook pattern
- `standalone_endpoint_test.py` - Self-contained script (no installation needed!)

## Error Handling

The library includes comprehensive error handling:

```python
try:
    response = client.chat("Hello!")
    print_response_content(response)
except requests.exceptions.HTTPError as e:
    print(f"HTTP Error: {e}")
except Exception as e:
    print(f"Error: {e}")
```

## Requirements

- Python 3.8+
- requests >= 2.31.0
- oci >= 2.100.0

