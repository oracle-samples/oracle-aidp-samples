# Usage Examples

## Running Locally with Default Profile

```bash
# Ensure your OCI config is set up at ~/.oci/config
streamlit run aidp_streamlit_chat.py
```

The application will:
1. Detect it's running locally (no resource principal environment)
2. Load the DEFAULT profile from `~/.oci/config`
3. Auto-detect if it's using API key or security token authentication
4. Display the authentication mode in the sidebar

## Running Locally with Custom Profile

```bash
# Set profile via environment variable
export OCI_CONFIG_PROFILE=my-profile
streamlit run aidp_streamlit_chat.py
```

Or configure directly in the Streamlit UI:
- Enter profile name in "OCI Profile" field
- Click "Init Client"

## Running Locally with Custom Config File

```bash
# Set custom config file location
export OCI_CONFIG_FILE=/path/to/custom/config
export OCI_CONFIG_PROFILE=my-profile
streamlit run aidp_streamlit_chat.py
```

## Running in Docker (Local Development)

```bash
# Build the image
docker build -t aidp-chat .

# Run with OCI config mounted (for testing locally)
docker run -p 8501:8501 \
  -v ~/.oci:/root/.oci:ro \
  -e AIDP_CHAT_URL="https://your-aidp-endpoint/chat" \
  -e OCI_CONFIG_PROFILE="DEFAULT" \
  aidp-chat
```

Access the application at http://localhost:8501

## Deploying to OCI Container Instance

### Step 1: Build and Push to OCIR

```bash
# Login to OCIR
docker login <region-key>.ocir.io
# Example: docker login iad.ocir.io

# Tag the image
docker tag aidp-chat <region-key>.ocir.io/<tenancy-namespace>/aidp-chat:latest

# Push to OCIR
docker push <region-key>.ocir.io/<tenancy-namespace>/aidp-chat:latest
```

### Step 2: Create Dynamic Group and Policies

Create a dynamic group for your container instances:

```
# Dynamic Group Rule
ALL {resource.type = 'computecontainerinstance', resource.compartment.id = 'ocid1.compartment...'}
```

Create policies for the dynamic group:

```
# Allow container instances to use AIDP service
Allow dynamic-group my-container-group to use generative-ai-family in compartment my-compartment

# If accessing other OCI services, add additional policies
Allow dynamic-group my-container-group to read objects in compartment my-compartment
```

### Step 3: Create Container Instance

Using OCI Console or CLI:

```bash
oci container-instances container-instance create \
  --compartment-id <compartment-ocid> \
  --display-name aidp-chat-app \
  --availability-domain <AD> \
  --shape CI.Standard.E4.Flex \
  --shape-config '{"ocpus": 1, "memoryInGBs": 4}' \
  --containers '[{
    "displayName": "aidp-chat",
    "imageUrl": "<region>.ocir.io/<namespace>/aidp-chat:latest",
    "environmentVariables": {
      "AIDP_CHAT_URL": "https://your-aidp-endpoint/chat"
    }
  }]' \
  --vnics '[{
    "subnetId": "<subnet-ocid>",
    "isPublicIpAssigned": true
  }]'
```

The container will automatically:
1. Detect the `OCI_RESOURCE_PRINCIPAL_VERSION` environment variable
2. Use resource principal authentication
3. No OCI config file needed!

### Step 4: Access the Application

Once the container instance is running:
1. Get the public IP address from the OCI Console
2. Access the application at `http://<public-ip>:8501`
3. The sidebar will show: **🔐 Auth Mode: Resource Principal (Container)**

## Programmatic Usage (Python)

### Local Mode with API Key

```python
from aidp_chat import AIDPChatClient, chat

# Using default profile
client = AIDPChatClient(
    aidp_url="https://your-aidp-endpoint/chat",
    config_file=None,  # Uses ~/.oci/config
    profile="DEFAULT"
)

# Make a chat request
response = chat(client, "What is OCI?", last_response_id=None)
print(response.json())
```

### Local Mode with Security Token

```python
from aidp_chat import AIDPChatClient, chat

# Using a profile with security token
client = AIDPChatClient(
    aidp_url="https://your-aidp-endpoint/chat",
    config_file=None,
    profile="TOKEN_PROFILE"  # Profile with security_token_file
)

response = chat(client, "Tell me about resource principals", last_response_id=None)
print(response.json())
```

### Container Mode (Automatic)

```python
from aidp_chat import AIDPChatClient, chat

# When running in OCI Container Instance, it automatically uses resource principal
client = AIDPChatClient(
    aidp_url="https://your-aidp-endpoint/chat"
)

# The client detects the environment and uses resource principal automatically
response = chat(client, "How does authentication work?", last_response_id=None)
print(response.json())
```

## Verifying Authentication Mode

The application provides clear feedback about which authentication mode is active:

### In Streamlit UI
Check the sidebar "Status" section:
- **🔐 Auth Mode: Resource Principal (Container)** - Using resource principal
- **🔐 Auth Mode: OCI Profile (profile-name)** - Using OCI profile with auto-detected auth type

### In Console Output
When the client is initialized, it prints:
- `"Detected OCI Container Instance environment - using resource principal authentication"`
- `"Using local OCI profile authentication (profile: DEFAULT)"`
- `"Detected API key authentication in profile 'DEFAULT'"`
- `"Detected security token authentication in profile 'TOKEN_PROFILE'"`

## Troubleshooting

### Issue: "Failed to create OCI signer"

**Local Mode:**
- Check that `~/.oci/config` exists and is readable
- Verify the profile name is correct
- Ensure private key file exists and has correct permissions
- For security tokens, verify the token file exists and is not expired

**Container Mode:**
- Verify the dynamic group includes your container instance
- Check that policies grant necessary permissions
- Ensure `OCI_RESOURCE_PRINCIPAL_VERSION` is set (automatic in OCI Container Instance)

### Issue: Authentication working locally but not in container

**Solution:**
- Verify you're using resource principal in the container (not mounting OCI config)
- Check dynamic group membership
- Review policy statements
- Check container logs for authentication errors

### Issue: Security token expired

**Solution:**
```bash
# Refresh your security token
oci session authenticate --profile TOKEN_PROFILE

# Or create a new session
oci session authenticate --region us-ashburn-1 --profile-name TOKEN_PROFILE
```
