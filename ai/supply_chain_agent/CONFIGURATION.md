# Configuration Guide - Supply Chain Agent

## 🔑 Required Configuration Values

You need **3 configuration values**:

1. **COMPARTMENT_ID** - OCI Compartment OCID
2. **ENDPOINT** - OCI Generative AI endpoint URL
3. **MODEL_ID** - AI model name

---

## 📍 Step 1: Find COMPARTMENT_ID

### Option A: From OCI Console
1. Login to OCI Console: https://cloud.oracle.com/
2. Navigate to **Identity & Security** > **Compartments**
3. Select your compartment
4. Copy the **OCID**
   ```
   Example: ocid1.compartment.oc1..aaaaaaaXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
   ```

### Option B: From AIDP (Easiest)
1. Login to AIDP Workbench
2. Check Settings or Profile section
3. Compartment OCID will be displayed there

### Option C: Environment Variable
```python
import os
COMPARTMENT_ID = os.getenv('AIDP_USER_COMPARTMENT_ID')
```

---

## 🌐 Step 2: Find ENDPOINT

### OCI Generative AI Endpoints by Region

| Region | Endpoint URL |
|--------|-------------|
| **US Phoenix** | `https://inference.generativeai.us-phoenix-1.oci.oraclecloud.com` |
| **US Chicago** | `https://inference.generativeai.us-chicago-1.oci.oraclecloud.com` |
| **Frankfurt** | `https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com` |
| **London** | `https://inference.generativeai.uk-london-1.oci.oraclecloud.com` |

### How to identify your region:
1. OCI Console > Check Region dropdown (top right)
2. Or check your AIDP Workbench URL
   - Example: `aidp.us-phoenix-1.oci.oraclecloud.com` → US Phoenix

---

## 🤖 Step 3: Choose MODEL_ID

### Available Models (OCI GenAI):

#### Fast Models (Quick responses)
```python
MODEL_ID = 'xai.grok-code-fast-1'  # Fast coding/queries
```

#### Powerful Models (Better reasoning)
```python
MODEL_ID = 'xai.grok-4'             # Best quality
MODEL_ID = 'cohere.command-r-plus'  # Good for RAG
MODEL_ID = 'meta.llama-3.1-405b-instruct'  # Strong reasoning
```

### To check model availability:
```
OCI Console > Analytics & AI > Generative AI
Available models will be listed there
```

---

## ✏️ Step 4: Update supply_chain_agent.py

```python
# Update lines 18-20

# Example 1: US Phoenix region
COMPARTMENT_ID = 'ocid1.compartment.oc1..aaaaaaaXXXXXXXXXXXXXXXXXXXXXXXX'
ENDPOINT = 'https://inference.generativeai.us-phoenix-1.oci.oraclecloud.com'
MODEL_ID = 'xai.grok-4'

# Example 2: Frankfurt region
COMPARTMENT_ID = 'ocid1.compartment.oc1..aaaaaaaYYYYYYYYYYYYYYYYYYYYYYYY'
ENDPOINT = 'https://inference.generativeai.eu-frankfurt-1.oci.oraclecloud.com'
MODEL_ID = 'cohere.command-r-plus'
```

---

## 📊 Step 5: Update Database Configuration

Update database config in **3 places** in the agent:

### Inventory Tool (Line ~38)
```python
inventory_sql_config = {
    "catalogKey": "your_catalog",      # ← Your AIDP catalog name
    "schemaKey": "your_schema",        # ← Your schema name
    "query": """..."""
}
```

### Orders Tool (Line ~60)
```python
order_sql_config = {
    "catalogKey": "your_catalog",      # ← Same catalog
    "schemaKey": "your_schema",        # ← Same schema
    "query": """..."""
}
```

### Suppliers Tool (Line ~80)
```python
supplier_sql_config = {
    "catalogKey": "your_catalog",      # ← Same catalog
    "schemaKey": "your_schema",        # ← Same schema
    "query": """..."""
}
```

---

## 🔍 How to find Database Catalog/Schema?

### Option A: From AIDP UI
1. AIDP Workbench > **Data** tab
2. Check left sidebar for catalogs
3. Expand catalog to see schemas

### Option B: SQL Query
```sql
-- Run in AIDP SQL editor
SELECT * FROM DBA_TABLES WHERE OWNER = 'YOUR_SCHEMA';
```

### Default Values (typical):
```
Catalog: aidp_default
Schema: aidpuser  (or your username)
```

---

## ✅ Complete Configuration Example

```python
# supply_chain_agent.py - Lines 18-20
COMPARTMENT_ID = 'ocid1.compartment.oc1..aaaaaaabcdefghijklmnopqrstuvwxyz123456'
ENDPOINT = 'https://inference.generativeai.us-phoenix-1.oci.oraclecloud.com'
MODEL_ID = 'xai.grok-4'

# Lines 38-39
inventory_sql_config = {
    "catalogKey": "aidp_default",
    "schemaKey": "aidpuser",
    "query": """..."""
}

# Lines 60-61
order_sql_config = {
    "catalogKey": "aidp_default",
    "schemaKey": "aidpuser",
    "query": """..."""
}

# Lines 80-81
supplier_sql_config = {
    "catalogKey": "aidp_default",
    "schemaKey": "aidpuser",
    "query": """..."""
}
```

---

## 🧪 Test Configuration

Test your configuration:

```python
# Test in AIDP Notebook
from aidputils.agents.toolkit.agent_helper import init_oci_llm
from aidputils.agents.toolkit.configs import OCIAIConf

llm_conf = OCIAIConf(
    model_provider='generic',
    compartment_id='your-compartment-ocid',
    endpoint='your-endpoint-url',
    model_id='xai.grok-4'
)

llm = init_oci_llm(llm_conf)
response = llm.invoke("Hello, test!")
print(response.content)
```

Common errors:
- ❌ **403 Forbidden**: Incorrect Compartment ID
- ❌ **404 Not Found**: Incorrect endpoint or region
- ❌ **Model not found**: MODEL_ID not available in your region

---

## 🆘 Troubleshooting

### Problem 1: "Compartment not found"
**Solution:**
- Verify compartment OCID in OCI Console
- Ensure GenAI service is enabled in compartment

### Problem 2: "Connection timeout"
**Solution:**
- Verify endpoint URL is correct
- Check region matches your setup
- Check network/firewall settings

### Problem 3: "Model not available"
**Solution:**
- Check OCI Console > GenAI service > Available models
- Choose a model available in your region

---

## 📝 Quick Reference Checklist

```
┌─────────────────────────────────────────────────────┐
│  CONFIGURATION CHECKLIST                            │
├─────────────────────────────────────────────────────┤
│  ☐ COMPARTMENT_ID (from OCI Console)               │
│  ☐ ENDPOINT (based on your region)                 │
│  ☐ MODEL_ID (available in your region)             │
│  ☐ catalogKey (from AIDP Data tab)                 │
│  ☐ schemaKey (your AIDP schema)                    │
└─────────────────────────────────────────────────────┘
```

---

**For additional help, contact AIDP Workbench support!** 🚀
