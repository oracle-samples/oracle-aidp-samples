"""
Basic chat example using AIDP Chat Client.

This example demonstrates:
- Loading OCI configuration
- Creating a client instance
- Sending a simple chat message
- Printing the response
"""

from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import print_response_content


def main():
    # Your AIDP agent endpoint URL
    aidp_url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"
    
    # Load OCI configuration
    config = load_oci_config("/Workspace/oci_config.ini")
    
    # Create client
    client = AIDPChatClient(aidp_url, config, auth="api_key")
    
    print("=" * 60)
    print("Basic Chat Example")
    print("=" * 60)
    
    # Send a simple message
    print("\nSending message: 'What can you help me with?'")
    response = client.chat("What can you help me with?", stream=False)
    
    # Print the response
    print_response_content(response)
    
    print("=" * 60)
    print("Example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
