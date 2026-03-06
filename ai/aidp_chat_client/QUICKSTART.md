# Quick Start Guide - AIDP Chat Client

Get started with AIDP Chat Client in 5 minutes!

## Prerequisites

- Python 3.8+
- OCI configuration file
- AIDP agent endpoint URL

## Option 1: Quick Test (No Installation Required)

If you just want to test quickly without installing anything:

1. **Navigate to the standalone example:**
   ```bash
   cd examples
   ```

2. **Edit the configuration in `standalone_endpoint_test.py`:**
   ```python
   # Update these lines:
   url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"
   config = oci.config.from_file("/path/to/your/oci_config.ini")
   ```

3. **Run it:**
   ```bash
   python standalone_endpoint_test.py
   ```

That's it! ✅

## Option 2: Install the Library

For production use or integration into your project:

### 1. Install the Package

```bash
cd aidp_chat_client
pip install -e .
```

### 2. Create Your First Script

Create a file called `my_chat.py`:

```python
from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import print_response_content

# Your endpoint URL
url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"

# Load config
config = load_oci_config("/path/to/oci_config.ini")

# Create client
client = AIDPChatClient(url, config)

# Chat!
response = client.chat("Hello, how are you?")
print_response_content(response)
```

### 3. Run Your Script

```bash
python my_chat.py
```

## Common Use Cases

### Simple Question

```python
response = client.chat("What is Python?")
print_response_content(response)
```

### Streaming Response

```python
response = client.chat("Tell me a long story", stream=True)
print_response_content(response)
```

### Conversation with Context

```python
from aidp_chat_client.utils import get_last_response_id

# First message
response1 = client.chat("What is AI?")
response_id = get_last_response_id(response1)

# Follow-up
response2 = client.chat("Can you explain more?", previous_response_id=response_id)
print_response_content(response2)
```

## Configuration File Format

Your OCI config file (`oci_config.ini`) should look like this:

```ini
[DEFAULT]
user=ocid1.user.oc1..aaaaaaaxxxxx
fingerprint=aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99
tenancy=ocid1.tenancy.oc1..aaaaaaaxxxxx
region=us-ashburn-1
key_file=/path/to/your/private_key.pem
```

## Where to Find Your Endpoint URL

Your AIDP agent endpoint URL follows this format:
```
https://gateway.aidp.{region}.oci.oraclecloud.com/agentendpoint/{agent-id}/chat
```

Example:
```
https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/13e012d7002943d9969587a237564d8d/chat
```

## Next Steps

- 📚 Read the [full documentation](README.md)
- 💡 Check out [more examples](examples/)
- 🔧 See [advanced usage](USAGE.md)
- 🐛 Report issues on GitHub

## Troubleshooting

### "No module named 'aidp_chat_client'"

**Solution:** Make sure you installed the package:
```bash
cd aidp_chat_client
pip install -e .
```

### "Authentication failed"

**Solution:** Check your OCI config file:
- Verify the user, tenancy, and fingerprint are correct
- Ensure the private key file exists and is readable
- Check that the path to the key file is correct

### "Connection timeout"

**Solution:** 
- Verify your endpoint URL is correct
- Check your network connection
- Ensure you have access to the AIDP service

## Support

Need help? Check:
- [Complete Usage Guide](USAGE.md)
- [Examples Directory](examples/)
- [README](README.md)

Happy coding! 🚀
