# AIDP Chat Streamlit Application

A Streamlit-based chat interface for interacting with Oracle Cloud Infrastructure (OCI) AI Digital Platform (AIDP) agents. This application provides an intuitive web interface for chat conversations with AIDP endpoints, with support for both local development and production deployment on OCI Container Instances.

## Features

- Interactive chat interface with conversation history
- **Streaming responses**: Real-time token-by-token display for more interactive experience
- Real-time communication with AIDP chat endpoints
- Comprehensive trace and span visualization for observability
- Support for multiple authentication modes:
  - **Local Development**: OCI API Key or Security Token authentication
  - **OCI Container Instance**: Resource Principal authentication
- Session management with configurable trace history
- Tree and table views for span analysis

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development](#local-development)
3. [OCI Container Instance Deployment](#oci-container-instance-deployment)
4. [Configuration](#configuration)
5. [Customizing Prompt Templates](#customizing-prompt-templates)
6. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### For Local Development
- Python 3.9 or later
- OCI CLI configured with valid credentials (`~/.oci/config`)
- Access to an AIDP chat endpoint
- pip package manager

### For OCI Container Instance Deployment
- OCI tenancy with appropriate permissions
- Docker installed locally for building images
- Access to OCI Container Registry (OCIR)
- AIDP chat endpoint URL
- Dynamic group and policy configuration for resource principal authentication

---

## Local Development

### 1. Setup Python Environment

```bash
# Clone or navigate to the project directory
cd /path/to/chat_container_streamlite

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure OCI Authentication

The application automatically detects your OCI authentication method from your OCI config file.

**Option A: API Key Authentication** (Default)

Ensure your `~/.oci/config` file contains:
```ini
[DEFAULT]
user=ocid1.user.oc1..aaaa...
fingerprint=xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx
key_file=~/.oci/oci_api_key.pem
tenancy=ocid1.tenancy.oc1..aaaa...
region=us-ashburn-1
```

**Option B: Security Token Authentication**

For security token-based authentication:
```ini
[DEFAULT]
user=ocid1.user.oc1..aaaa...
fingerprint=xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx:xx
key_file=~/.oci/oci_api_key.pem
security_token_file=~/.oci/token
tenancy=ocid1.tenancy.oc1..aaaa...
region=us-ashburn-1
```

### 3. Set AIDP Endpoint

Set the AIDP chat endpoint URL as an environment variable:

```bash
export AIDP_CHAT_URL="https://your-aidp-endpoint.oci.oraclecloud.com/chat"
```

Or configure it in the Streamlit sidebar after launching the application.

### 4. Run the Application

```bash
streamlit run aidp_streamlit_chat.py --server.port=8501
```

Open your browser to `http://localhost:8501` to access the chat interface.

### 5. Using the Application

1. In the sidebar, verify or enter your AIDP Chat URL
2. Optionally configure OCI profile settings (defaults to `DEFAULT` profile)
3. **Toggle streaming mode**: Enable or disable "Enable Streaming" checkbox for real-time token-by-token responses
4. Click **"Init Client"** to initialize the connection
5. Start chatting in the main panel
6. View traces and spans in the sidebar's trace panel

#### Streaming vs Non-Streaming Mode

- **Streaming Enabled** (default): Responses appear token-by-token as they're generated, providing immediate feedback and a more interactive experience
- **Streaming Disabled**: Responses appear all at once after generation completes, which may be faster for short responses

Both modes fully support trace visualization and conversation history.

---

## OCI Container Instance Deployment

This section covers deploying the application to OCI Container Instances with resource principal authentication.

### Step 1: Build and Push Docker Image

#### 1.1 Build the Docker Image

```bash
# Navigate to project directory
cd /path/to/chat_container_streamlite

# Build the image
docker build -t aidp-chat-streamlit:latest .
```

#### 1.2 Login to Oracle Container Registry (OCIR)

```bash
# Format: docker login <region-key>.ocir.io
# Username format: <tenancy-namespace>/<oci-username>
# Password: Auth token from OCI Console (Identity > Users > User Details > Auth Tokens)

docker login <region-key>.ocir.io
```

**Example**:
```bash
docker login us-ashburn-1.ocir.io
Username: mytenancy/john.doe@example.com
Password: <your-auth-token>
```

Common region keys:
- `us-ashburn-1.ocir.io` (Ashburn)
- `us-phoenix-1.ocir.io` (Phoenix)
- `uk-london-1.ocir.io` (London)
- `ap-tokyo-1.ocir.io` (Tokyo)

#### 1.3 Tag and Push the Image

```bash
# Tag the image
docker tag aidp-chat-streamlit:latest \
  <region-key>.ocir.io/<tenancy-namespace>/aidp-chat-streamlit:latest

# Push to OCIR
docker push <region-key>.ocir.io/<tenancy-namespace>/aidp-chat-streamlit:latest
```

**Example**:
```bash
docker tag aidp-chat-streamlit:latest \
  us-ashburn-1.ocir.io/mytenancy/aidp-chat-streamlit:latest

docker push us-ashburn-1.ocir.io/mytenancy/aidp-chat-streamlit:latest
```

### Step 2: Configure Dynamic Group for Resource Principal

The container instance needs a resource principal to authenticate with AIDP endpoints.

#### 2.1 Create a Dynamic Group

1. Navigate to **Identity & Security** > **Domains** > **Default Domain** > **Dynamic Groups**
2. Click **Create Dynamic Group**
3. Provide a name: `aidp-chat-container-instances`
4. Description: `Dynamic group for AIDP chat container instances`
5. Add matching rules:

**Match all container instances in a specific compartment:**
```
ALL {resource.type='computecontainerinstance', resource.compartment.id='ocid1.compartment.oc1..aaaa...'}
```

**Match a specific container instance by OCID:**
```
resource.id='ocid1.computecontainerinstance.oc1.iad.aaaa...'
```

**Match by tag (recommended for production):**
```
ALL {resource.type='computecontainerinstance', tag.app.name='aidp-chat'}
```

6. Click **Create**

#### 2.2 Note the Dynamic Group OCID

Copy the OCID of the created dynamic group - you'll need it for AIDP policy configuration.

### Step 3: Configure AIDP Permissions

The dynamic group needs to be granted access to the AIDP service. This can be done in two ways:

#### Option A: Add Dynamic Group Directly to AIDP Permissions

1. Navigate to your **AIDP Console** (product-specific interface)
2. Go to **Permissions** or **Access Control**
3. Click **Add Principal**
4. Select **Dynamic Group**
5. Enter the dynamic group OCID or name: `aidp-chat-container-instances`
6. Assign required permissions:
   - `chat:invoke` (required for chat endpoint access)
   - `session:read` (optional, for session management)
7. Click **Save**

#### Option B: Add Dynamic Group to an AIDP Role

1. Navigate to **AIDP Console** > **Roles**
2. Select or create a role (e.g., `chat-user-role`)
3. Ensure the role has necessary permissions:
   - `chat:invoke`
   - Any additional permissions required
4. Go to **Role Members** or **Principals**
5. Click **Add Member**
6. Select **Dynamic Group**
7. Choose `aidp-chat-container-instances`
8. Click **Add**

### Step 4: Create OCI Container Instance

#### 4.1 Using OCI Console

1. Navigate to **Developer Services** > **Container Instances**
2. Click **Create Container Instance**
3. Provide basic information:
   - **Name**: `aidp-chat-app`
   - **Compartment**: Select your compartment
   - **Availability Domain**: Choose an AD
   - **Shape**: Select appropriate shape (e.g., `CI.Standard.E4.Flex`)
   - **Configure shape** (if applicable): Set OCPUs and memory

4. Configure networking:
   - **Virtual Cloud Network**: Select your VCN
   - **Subnet**: Choose a public subnet (for internet access)
   - **Public IP**: Enable (if you want direct access)

5. Add container:
   - **Container name**: `chat-app`
   - **Image**: `<region-key>.ocir.io/<tenancy-namespace>/aidp-chat-streamlit:latest`
   - **Image registry**: Select OCIR
   - **Image pull authentication**: Select appropriate auth method

6. Configure container:
   - **Port mapping**: Add port `8501` (TCP)
   - **Environment variables**:
     ```
     AIDP_CHAT_URL=https://your-aidp-endpoint.oci.oraclecloud.com/chat
     ```

7. **Tagging** (optional but recommended):
   - Add tag: `app.name = aidp-chat` (matches dynamic group rule)

8. Review and click **Create**

#### 4.2 Using OCI CLI

```bash
oci container-instances container-instance create \
  --compartment-id ocid1.compartment.oc1..aaaa... \
  --availability-domain "AD-1" \
  --shape "CI.Standard.E4.Flex" \
  --shape-config '{"ocpus":1.0,"memory_in_gbs":8.0}' \
  --vnics '[{"subnetId":"ocid1.subnet.oc1..aaaa...","isPublicIpAssigned":true}]' \
  --containers '[{
      "displayName":"chat-app",
      "imageUrl":"us-ashburn-1.ocir.io/mytenancy/aidp-chat-streamlit:latest",
      "environmentVariables":{
        "AIDP_CHAT_URL":"https://your-aidp-endpoint.oci.oraclecloud.com/chat"
      }
    }]' \
  --display-name "aidp-chat-app" \
  --freeform-tags '{"app.name":"aidp-chat"}'
```

#### 4.3 Access the Application

1. Get the public IP of the container instance:
   - In Console: Navigate to the container instance details
   - Or via CLI: `oci container-instances container-instance get --container-instance-id <ocid>`

2. Access the application:
   ```
   http://<public-ip>:8501
   ```

3. For production, set up a load balancer with HTTPS:
   - Create an OCI Load Balancer
   - Configure backend set pointing to container instance IP:8501
   - Set up SSL certificate
   - Configure security lists/NSGs for load balancer

### Step 5: Verify Resource Principal Authentication

Once the container instance is running:

1. Access the Streamlit application via the public IP
2. In the sidebar, check the **Status** section
3. You should see:
   ```
   🔐 Auth Mode: Resource Principal (Container)
   ```

4. Initialize the client and test chat functionality
5. If authentication fails, verify:
   - Dynamic group rules match your container instance
   - AIDP permissions are correctly configured
   - Container instance has internet connectivity to reach AIDP endpoint

---

## Configuration

### Environment Variables

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `AIDP_CHAT_URL` | Full URL to AIDP chat endpoint (must end with `/chat`) | Yes | None |
| `OCI_CONFIG_FILE` | Path to OCI config file | No | `~/.oci/config` |
| `OCI_CONFIG_PROFILE` | OCI profile name to use | No | `DEFAULT` |
| `OCI_RESOURCE_PRINCIPAL_VERSION` | Auto-set by OCI Container Instance | No | None |

### Application Configuration

Configure these settings in the Streamlit sidebar:

- **AIDP Chat URL**: Full endpoint URL for AIDP chat service
- **OCI Config File**: Override default OCI config location (local mode only)
- **OCI Profile**: Select which profile to use from config (local mode only)

### Authentication Modes

The application automatically detects the authentication mode:

1. **Resource Principal** (Container Instance):
   - Automatically used when `OCI_RESOURCE_PRINCIPAL_VERSION` environment variable is present
   - No configuration file needed
   - Requires dynamic group and AIDP policy configuration

2. **API Key** (Local):
   - Uses standard OCI config file
   - Detected when config has `user`, `fingerprint`, `key_file`

3. **Security Token** (Local):
   - Uses OCI config with security token
   - Detected when config has `security_token_file`

---

## Customizing Prompt Templates

The application supports customizable prompt templates loaded from a JSON file. This allows you to define frequently-used prompts with variable placeholders.

### Template File Format

Create a `prompt_templates.json` file with this structure:

```json
{
  "Template Name": {
    "template": "Your prompt text with {variable} placeholders",
    "description": "Optional description"
  }
}
```

**Example:**

```json
{
  "Generate SQL Query": {
    "template": "Generate a SQL query to {task}.\n\nTable schema:\n{schema}\n\nRequirements:\n{requirements}",
    "description": "Create SQL queries based on requirements and schema"
  }
}
```

### Template File Locations

The app searches for `prompt_templates.json` in these locations (in order):

1. Current working directory: `./prompt_templates.json`
2. Application directory: `<app_dir>/prompt_templates.json`
3. Docker mount: `/app/prompt_templates.json`
4. Custom path: `$TEMPLATES_PATH/prompt_templates.json`

### Using Custom Templates Locally

1. Edit `prompt_templates.json` in the app directory
2. Click the "🔄 Reload" button in the sidebar, or restart the app
3. Your templates will appear in the template selector

### Using Custom Templates with Docker

**Option 1: Mount a custom templates file**

```bash
docker run -v /path/to/your/prompt_templates.json:/app/prompt_templates.json \
           -p 8501:8501 \
           -e AIDP_CHAT_URL="your-endpoint-url" \
           your-image-name
```

**Option 2: Use custom path with environment variable**

```bash
docker run -v /path/to/templates:/custom/templates \
           -e TEMPLATES_PATH=/custom/templates \
           -e AIDP_CHAT_URL="your-endpoint-url" \
           -p 8501:8501 \
           your-image-name
```

**Option 3: Build with custom templates**

```dockerfile
FROM your-image-name
COPY my_custom_templates.json /app/prompt_templates.json
```

### Template Levels

The app includes two sets of pre-built templates:

#### **Basic Templates (Default)** - `prompt_templates.json`
Perfect for beginners and everyday data tasks (15 templates):
- Getting Started - Learn data concepts
- Explore Dataset - Understand new data
- Simple Data Summary - Quick insights
- Basic SQL Query - Generate queries
- Visualize Data - Choose charts
- Find Patterns - Discover relationships
- Clean Data - Fix data issues
- Compare Groups - Analyze differences
- Calculate Metrics - Compute KPIs
- Data Question - Ask anything
- Predict Outcome - Make forecasts
- Interpret Results - Understand outputs
- Join Tables - Combine data
- Filter Data - Find specific records
- Handle Missing Data - Deal with nulls

See **[GETTING_STARTED.md](GETTING_STARTED.md)** for detailed usage examples!

#### **Advanced Templates** - `prompt_templates.advanced.json`
For ML engineers and data scientists (15 templates):
- Generate SQL Query (complex)
- Explain Model Results
- Data Quality Check
- Feature Engineering
- Optimize Pipeline
- Debug Model Performance
- Compare Models
- Data Transformation
- Analyze Time Series
- RAG Query Optimization
- Hyperparameter Tuning
- A/B Test Analysis
- Production Deployment
- Data Drift Detection
- Cost Optimization

**To use advanced templates:** Copy `prompt_templates.advanced.json` to `prompt_templates.json`

### Template Variables

Use `{variable_name}` in templates for user-provided values. The app automatically detects variables and creates input fields.

**Example usage:**

1. Select "Generate SQL Query" template
2. Fill in `{task}`, `{schema}`, `{requirements}`
3. Click "📤 Use Template"
4. Click "📤 Send" to submit to the AI

For more details, see [TEMPLATES.md](TEMPLATES.md).

---

## Troubleshooting

### Local Development Issues

**Issue: "Failed to initialize AIDP client"**
- Verify `~/.oci/config` exists and is properly formatted
- Check that key file path is correct and accessible
- Ensure OCI profile name matches config file section
- Validate network connectivity to OCI services

**Issue: "Failed to create OCI signer"**
- For API key auth: Verify fingerprint matches the key file
- For security token: Ensure token file exists and is not expired
- Check file permissions on key and token files

**Issue: Chat endpoint returns 401/403**
- Verify the OCI principal (user) has proper AIDP permissions
- Check that AIDP_CHAT_URL is correct and accessible
- Ensure the user/group has `chat:invoke` permission in AIDP

### Container Instance Issues

**Issue: Container fails to start**
- Check container logs in OCI Console
- Verify image was pushed correctly to OCIR
- Ensure container has sufficient memory/CPU resources

**Issue: "Resource Principal authentication failed"**
- Verify dynamic group rules match the container instance
- Check that dynamic group has been added to AIDP permissions/role
- Confirm container instance is in the correct compartment
- Validate AIDP endpoint is reachable from container subnet

**Issue: Cannot access application on port 8501**
- Verify security list allows ingress on TCP port 8501
- Check NSG rules if using Network Security Groups
- Ensure container instance has public IP or is behind load balancer
- Confirm container is running: check status in Console

**Issue: "Auth Mode: Resource Principal" not showing**
- Check environment variables in container configuration
- Review container instance logs for startup errors
- Verify OCI_RESOURCE_PRINCIPAL_VERSION is set (auto-set by OCI)

### Network and Connectivity

**Issue: Cannot reach AIDP endpoint from container**
- Verify VCN has internet gateway and route table configured
- Check that subnet security lists allow outbound HTTPS (443)
- Ensure DNS resolution works in the container
- Test connectivity using a debug container in the same subnet

### AIDP Permission Issues

**Issue: "Permission denied" when calling chat endpoint**
- Verify dynamic group membership (instance OCID matches rules)
- Check AIDP console for policy assignments
- Ensure role (if used) has correct permissions
- Validate policy statements are in the correct compartment

---

## Architecture Overview

### Authentication Flow

```
Local Development:
User -> Streamlit App -> AIDPChatClient -> OCI SDK (API Key/Token) -> AIDP Endpoint

Container Instance:
User -> Streamlit App -> AIDPChatClient -> OCI SDK (Resource Principal) -> AIDP Endpoint
                                                          ↑
                                                 Dynamic Group -> AIDP Policies
```

### Key Components

- **aidp_streamlit_chat.py**: Main Streamlit UI application
- **aidp_chat.py**: AIDP client library with automatic auth detection
- **Dockerfile**: Container image definition
- **requirements.txt**: Python dependencies

---

## Security Best Practices

1. **Never commit credentials**: Keep OCI config and keys out of version control
2. **Use resource principals in production**: Avoid embedding API keys in containers
3. **Principle of least privilege**: Grant only necessary AIDP permissions
4. **Use private subnets**: Place containers in private subnets with NAT gateway
5. **Enable HTTPS**: Use load balancers with SSL termination for production
6. **Regular updates**: Keep base images and dependencies updated
7. **Secret management**: Use OCI Vault for sensitive configuration

---

## Additional Resources

- [OCI Container Instances Documentation](https://docs.oracle.com/en-us/iaas/Content/container-instances/home.htm)
- [OCI Resource Principal Authentication](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_authentication_methods.htm#sdk_authentication_methods_resource_principal)
- [OCI Dynamic Groups](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingdynamicgroups.htm)
- [Streamlit Documentation](https://docs.streamlit.io/)

---

## File Structure

```
chat_container_streamlite/
├── aidp_streamlit_chat.py    # Main Streamlit application
├── aidp_chat.py               # AIDP client with auth handling
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container build definition
└── README.md                  # This file
