"""
Complete Endpoint Example - Based on original test_agent_end_points notebook.

This example demonstrates:
- Complete usage matching the original notebook
- Both streaming and non-streaming chat
- Follow-up questions with context
- Full endpoint testing workflow
"""

import sys
sys.path.append('..')

from aidp_chat_client import AIDPChatClient
from aidp_chat_client.utils import (
    get_last_response_id, 
    print_response_content, 
    get_traces
)
import oci


def main():
    # Your AIDP agent endpoint URL (replace with your actual endpoint)
    url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/13e012d7002943d9969587a237564d8d/chat"
    
    # Load OCI config
    config = oci.config.from_file("/Workspace/oci_config.ini")
    auth = ""  # or "api_key" / "security_token"
    
    # Create client
    client = AIDPChatClient(url, config, auth)
    
    print("=" * 60)
    print("Testing AIDP Chat")
    print("=" * 60)
    
    # Example 1: Using streaming chat (recommended for real-time responses)
    print("\n### Example 1: Streaming Chat ###")
    response = client.chat("Tell me about plants", stream=True)
    print_response_content(response)
    
    # Get response ID for follow-up
    response_id = get_last_response_id(response)
    print(f"Response ID: {response_id}")
    
    # Example 2: Follow-up question with context
    if response_id:
        print("\n### Example 2: Follow-up Question ###")
        followup = client.chat("Tell me more about their benefits", previous_response_id=response_id, stream=True)
        print_response_content(followup)
        
        # Get traces if available
        traces = get_traces(followup)
        if traces:
            print("\nTrace Information:")
            print(traces)
    
    # Example 3: Non-streaming chat (simpler but slower to start)
    print("\n### Example 3: Non-Streaming Chat ###")
    simple_response = client.chat("What can you help me with?", stream=False)
    print_response_content(simple_response)
    
    print("\n" + "=" * 60)
    print("✅ Testing complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
