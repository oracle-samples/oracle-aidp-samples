"""
Configuration management for AIDP Chat Client.
"""

import os
from typing import Dict, Any, Optional
import oci


def load_oci_config(config_path: Optional[str] = None, profile: str = "DEFAULT") -> Dict[str, Any]:
    """
    Load OCI configuration from a file.
    
    Args:
        config_path: Path to the OCI config file (defaults to ~/.oci/config)
        profile: Profile name to use from the config file
        
    Returns:
        Configuration dictionary
    """
    if config_path:
        config = oci.config.from_file(config_path, profile)
    else:
        config = oci.config.from_file(profile_name=profile)
    
    return config


def validate_config(config: Dict[str, Any], auth_type: str = "api_key") -> bool:
    """
    Validate the OCI configuration dictionary.
    
    Args:
        config: Configuration dictionary to validate
        auth_type: Type of authentication ("api_key" or "security_token")
        
    Returns:
        True if configuration is valid
        
    Raises:
        ValueError: If configuration is missing required fields
    """
    if auth_type == "api_key":
        required_keys = ["tenancy", "user", "fingerprint", "key_file"]
    else:  # security_token
        required_keys = ["security_token_file", "key_file"]
    
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    # Check if key file exists
    if "key_file" in config and not os.path.exists(config["key_file"]):
        raise ValueError(f"Private key file not found: {config['key_file']}")
    
    # Check if security token file exists (for security_token auth)
    if auth_type == "security_token" and "security_token_file" in config:
        if not os.path.exists(config["security_token_file"]):
            raise ValueError(f"Security token file not found: {config['security_token_file']}")
    
    return True


def create_client_from_config(
    aidp_url: str,
    config_path: Optional[str] = None,
    profile: str = "DEFAULT",
    auth_type: str = "api_key"
):
    """
    Create an AIDP Chat Client from configuration file.
    
    Args:
        aidp_url: The AIDP agent endpoint URL
        config_path: Path to the OCI config file
        profile: Profile name to use
        auth_type: Authentication type ("api_key" or "security_token")
        
    Returns:
        Configured AIDPChatClient instance
    """
    from .client import AIDPChatClient
    
    config = load_oci_config(config_path, profile)
    validate_config(config, auth_type)
    
    return AIDPChatClient(aidp_url, config, auth_type)
