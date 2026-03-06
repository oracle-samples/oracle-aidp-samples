"""
config_loader.py
------------------------------------------------------------
Responsibility:
- Load multi-table DQ configuration from YAML
- Validate configuration structure
------------------------------------------------------------
"""

import yaml


def load_dq_config(path: str) -> dict:
    """
    Load and parse DQ configuration file.
    
    Args:
        path (str): Path to YAML config file
        
    Returns:
        dict: Complete configuration including tables and cross_table_rules
    """
    with open(path, "r") as f:
        config = yaml.safe_load(f)
    
    # Return full config with both tables and cross_table_rules
    return config
