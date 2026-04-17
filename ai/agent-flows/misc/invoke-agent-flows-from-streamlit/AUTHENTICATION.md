# OCI Authentication Modes

This chat application supports two authentication modes for OCI:

## 1. Local Run Mode (OCI Profile)

When running locally, the application uses OCI profile authentication from your `~/.oci/config` file.

### Supported Authentication Types

The application automatically introspects your OCI profile and detects:

- **API Key Authentication**: Standard OCI authentication with API keys
- **Security Token Authentication**: Session-based authentication using security tokens

### Configuration

1. Ensure you have a valid OCI config file at `~/.oci/config` (or specify a custom path)
2. The application will automatically detect which authentication type your profile uses
3. Configure in the Streamlit UI:
   - **OCI Config File**: Leave blank to use default `~/.oci/config`, or specify custom path
   - **OCI Profile**: Specify profile name (default: "DEFAULT")

### Example OCI Config Profiles

**API Key Profile:**
```ini
[DEFAULT]
user=ocid1.user.oc1..xxx
fingerprint=xx:xx:xx:xx:xx
tenancy=ocid1.tenancy.oc1..xxx
region=us-ashburn-1
key_file=~/.oci/oci_api_key.pem
```

**Security Token Profile:**
```ini
[TOKEN_PROFILE]
security_token_file=~/.oci/sessions/token
key_file=~/.oci/sessions/key.pem
region=us-ashburn-1
```

## 2. Container Mode (Resource Principal)

When running inside OCI Container Instance service, the application automatically uses **Resource Principal** authentication.

### How it Works

- The application detects the `OCI_RESOURCE_PRINCIPAL_VERSION` environment variable
- When detected, it automatically switches to resource principal authentication
- No OCI config file or profile is needed in container mode

### Container Deployment

When deploying to OCI Container Instance:

1. Build and push the Docker image to OCIR
2. Create a Container Instance with the image
3. Ensure the Container Instance has a dynamic group with appropriate policies
4. The application will automatically use resource principal authentication

### Example Dynamic Group Policy

```
Allow dynamic-group my-container-group to use generative-ai-family in compartment my-compartment
```

## Authentication Mode Detection

The application shows the current authentication mode in the Streamlit sidebar:

- **🔐 Auth Mode: Resource Principal (Container)** - Running in OCI Container Instance
- **🔐 Auth Mode: OCI Profile (PROFILE_NAME)** - Running locally with specified profile
  - Auth type (API key/security token) is auto-detected from the profile

## Running the Application

### Local Mode

```bash
# Using default profile
streamlit run aidp_streamlit_chat.py

# Using custom profile
export OCI_CONFIG_PROFILE=my-profile
streamlit run aidp_streamlit_chat.py

# Using custom config file
export OCI_CONFIG_FILE=/path/to/custom/config
streamlit run aidp_streamlit_chat.py
```

### Container Mode

```bash
# Build the image
docker build -t aidp-chat .

# Run locally (simulates container but still uses OCI profile)
docker run -p 8501:8501 \
  -v ~/.oci:/root/.oci:ro \
  -e AIDP_CHAT_URL="your-aidp-endpoint" \
  aidp-chat

# Deploy to OCI Container Instance (uses resource principal automatically)
# - Push image to OCIR
# - Create Container Instance with image
# - Set AIDP_CHAT_URL environment variable
# - Resource principal authentication is automatic
```

## Troubleshooting

### Local Mode Issues

**Error: "Failed to create OCI signer"**
- Verify your OCI config file exists and is properly formatted
- Check file permissions on your private key file
- Ensure the profile name is correct

**Security Token Expired**
- Run `oci session authenticate` to refresh your security token
- Update the token in your OCI config file

### Container Mode Issues

**Error: "No service account token found"**
- Ensure the Container Instance has a dynamic group assigned
- Verify the dynamic group has appropriate policies
- Check that the Container Instance is properly configured

**Authentication still using profile in container**
- Verify that `OCI_RESOURCE_PRINCIPAL_VERSION` environment variable is set by OCI
- This is automatically set by OCI Container Instance service
