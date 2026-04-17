#!/usr/bin/env python3
"""
Simple test script to debug AIDP streaming responses.
Run this to see the debug output from the streaming client.
"""
import os
import sys
from aidp_chat import AIDPChatClient, chat_stream

def test_streaming():
    # Get URL from environment or use default
    aidp_url = os.getenv("AIDP_CHAT_URL", "")

    if not aidp_url:
        print("ERROR: AIDP_CHAT_URL environment variable not set")
        print("Please set it to your AIDP chat endpoint URL")
        sys.exit(1)

    print(f"Using AIDP URL: {aidp_url}")
    print("=" * 60)

    try:
        # Create client
        client = AIDPChatClient(aidp_url, config_file=None, profile="DEFAULT")
        print("Client created successfully")
        print("=" * 60)

        # Test streaming
        test_message = "Say 'Hello, World!' and nothing else."
        print(f"Sending message: {test_message}")
        print("=" * 60)
        print("Starting stream...")
        print("-" * 60)

        stream = chat_stream(client, test_message, None)

        chunk_count = 0
        total_text = ""

        # Iterate through stream
        for chunk in stream:
            chunk_count += 1
            total_text += chunk
            print(f"[CHUNK {chunk_count}]: '{chunk}'", flush=True)

        print("-" * 60)
        print(f"Stream complete. Total chunks: {chunk_count}")
        print(f"Total text length: {len(total_text)}")
        print(f"Total text: '{total_text}'")
        print("=" * 60)

        # Get response data
        response_data = stream.get_response_data()
        print(f"Response ID: {response_data.get('id')}")
        print(f"Accumulated text: '{response_data.get('accumulated_text')}'")

        if chunk_count == 0:
            print("\n⚠️  WARNING: No chunks were yielded!")
            print("Check the DEBUG output above to see what was received from the API.")
        else:
            print(f"\n✓ SUCCESS: Received {chunk_count} chunks")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    test_streaming()
