"""
Conversation with context example using AIDP Chat Client.

This example demonstrates:
- Multi-turn conversations
- Maintaining context between messages
- Using response IDs for conversation continuity
"""

from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import print_response_content, get_last_response_id


def main():
    # Your AIDP agent endpoint URL
    aidp_url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"
    
    # Load OCI configuration
    config = load_oci_config("/Workspace/oci_config.ini")
    
    # Create client
    client = AIDPChatClient(aidp_url, config, auth="api_key")
    
    print("=" * 60)
    print("Conversation with Context Example")
    print("=" * 60)
    
    # First message
    print("\n### Message 1 ###")
    print("User: 'Tell me about renewable energy'")
    response1 = client.chat("Tell me about renewable energy", stream=True)
    print_response_content(response1)
    
    # Get response ID for context
    response_id = get_last_response_id(response1)
    print(f"\nResponse ID: {response_id}")
    
    # Second message with context
    if response_id:
        print("\n### Message 2 (with context) ###")
        print("User: 'What are the main types?'")
        response2 = client.chat(
            "What are the main types?", 
            previous_response_id=response_id,
            stream=True
        )
        print_response_content(response2)
        
        # Get new response ID
        response_id = get_last_response_id(response2)
        
        # Third message with accumulated context
        print("\n### Message 3 (with accumulated context) ###")
        print("User: 'Which one is most efficient?'")
        response3 = client.chat(
            "Which one is most efficient?",
            previous_response_id=response_id,
            stream=True
        )
        print_response_content(response3)
    
    print("\n" + "=" * 60)
    print("✅ Conversation example complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
