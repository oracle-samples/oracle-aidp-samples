"""
Streaming chat example using AIDP Chat Client.

This example demonstrates:
- Using streaming responses for real-time output
- Handling streamed data
- Comparing streaming vs non-streaming responses
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
    print("Streaming Chat Example")
    print("=" * 60)
    
    # Example 1: Streaming response
    print("\n### Example 1: Streaming Response ###")
    print("Question: 'Tell me about artificial intelligence'")
    response = client.chat("Tell me about artificial intelligence", stream=True)
    print_response_content(response)
    
    # Example 2: Non-streaming response (for comparison)
    print("\n### Example 2: Non-Streaming Response (for comparison) ###")
    print("Question: 'What is machine learning?'")
    response = client.chat("What is machine learning?", stream=False)
    print_response_content(response)
    
    print("\n" + "=" * 60)
    print("✅ Streaming example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
