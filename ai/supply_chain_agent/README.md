# Supply Chain Agent for Oracle AIDP

Multi-agent AI system for supply chain operations.

## Quick Start

### 1. Generate Data
```bash
python generate_data.py
```
Output: 170 records in CSV/JSON

### 2. Create Tables in AIDP
Run `create_tables.sql` in AIDP SQL editor

### 3. Import Data
Import CSV files to AIDP tables

### 4. Configure Agent
See `CONFIGURATION.md` for detailed setup guide.

Quick config:
```python
# In supply_chain_agent.py (lines 18-20)
COMPARTMENT_ID = 'ocid1.compartment.oc1..aaaaaaXXXXX'  # From OCI Console
ENDPOINT = 'https://inference.generativeai.us-phoenix-1.oci.oraclecloud.com'  # Your region
MODEL_ID = 'xai.grok-4'  # Available model

# Update database config (lines 38, 60, 80)
"catalogKey": "your_aidp_catalog"  # From AIDP Data tab
"schemaKey": "your_schema"  # Your AIDP schema
```

📖 **Read CONFIGURATION.md for:**
- How to find COMPARTMENT_ID
- Endpoint URLs by region
- Available MODEL_IDs
- Database catalog/schema setup

### 5. Deploy
Upload `supply_chain_agent.py` to AIDP Workbench

## Files
- `supply_chain_agent.py` - Main agent
- `supply_chain_config.yaml` - Data config
- `create_tables.sql` - Table creation
- `generate_data.py` - Data generator
- `README.md` - This file

## Test Queries
- "What's the inventory in warehouse WH-001?"
- "Show delayed orders"
- "Which suppliers have best performance?"

Total: 5 files only!
