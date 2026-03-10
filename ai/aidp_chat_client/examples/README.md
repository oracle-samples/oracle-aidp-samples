# AIDP Chat Client Examples

This directory contains example scripts demonstrating various features of the AIDP Chat Client library.

## Prerequisites

Before running these examples:

1. Install the AIDP Chat Client library:
   ```bash
   cd ../
   pip install -e .
   ```

2. Configure your OCI credentials in a config file (e.g., `/Workspace/oci_config.ini`)

3. Update the `aidp_url` variable in each example with your AIDP agent endpoint URL

## Examples

### 1. basic_chat.py

Demonstrates the most basic usage of the client:
- Loading configuration
- Creating a client
- Sending a simple message
- Printing the response

**Run:**
```bash
python basic_chat.py
```

### 2. streaming_chat.py

Shows how to use streaming responses:
- Streaming vs non-streaming responses
- Real-time response handling
- Performance comparison

**Run:**
```bash
python streaming_chat.py
```

### 3. conversation_context.py

Demonstrates multi-turn conversations:
- Maintaining conversation context
- Using response IDs
- Building on previous messages

**Run:**
```bash
python conversation_context.py
```

### 4. batch_processing.py

Shows how to process multiple queries:
- Batch query processing
- Error handling for multiple requests
- Results aggregation
- Rate limiting considerations

**Run:**
```bash
python batch_processing.py
```

### 5. complete_endpoint_example.py

Complete example matching the original notebook:
- Full endpoint testing workflow
- Both streaming and non-streaming chat
- Follow-up questions with context
- Trace information extraction
- Matches the original test_agent_end_points notebook pattern

**Run:**
```bash
python complete_endpoint_example.py
```

### 6. standalone_endpoint_test.py

Standalone version that doesn't require library installation:
- Self-contained script with all functions inline
- Can run independently without installing the package
- Perfect for quick testing or environments where pip install isn't available
- Includes all functionality from the original notebook

**Run:**
```bash
python standalone_endpoint_test.py
```

## Configuration

### For Library-based Examples (1-4)

Each example expects an OCI configuration file. Update the path in the examples:

```python
config = load_oci_config("/Workspace/oci_config.ini")
```

### For Complete Endpoint Example (5)

Update both the URL and config path:

```python
# Your AIDP agent endpoint URL
url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"

# Load OCI config
config = oci.config.from_file("/Workspace/oci_config.ini")
auth = ""  # or "api_key" / "security_token"
```

### For Standalone Version (6)

Edit the configuration section at the bottom of the file:

```python
# Your AIDP agent endpoint URL
url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"

# Load OCI config
config = oci.config.from_file("/Workspace/oci_config.ini")
auth = ""  # "" or "api_key" or "security_token"
```

### Example OCI Config File

```ini
[DEFAULT]
user=ocid1.user.oc1..aaaaaaaxxxxx
fingerprint=aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
tenancy=ocid1.tenancy.oc1..aaaaaaaxxxxx
region=us-ashburn-1
key_file=/path/to/your/private_key.pem
```

## Customization

Feel free to modify these examples for your use case:

- Change the `aidp_url` to your agent endpoint
- Modify queries to test different scenarios
- Adjust streaming vs non-streaming based on your needs
- Add error handling specific to your application

## Support

For issues or questions about these examples, please refer to the main [README.md](../README.md) or open an issue on GitHub.
