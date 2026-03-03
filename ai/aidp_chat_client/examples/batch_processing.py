"""
Batch processing example using AIDP Chat Client.

This example demonstrates:
- Processing multiple queries
- Collecting and aggregating results
- Error handling for batch operations
"""

from aidp_chat_client import AIDPChatClient
from aidp_chat_client.config import load_oci_config
from aidp_chat_client.utils import extract_text_content
import time


def main():
    # Your AIDP agent endpoint URL
    aidp_url = "https://gateway.aidp.us-ashburn-1.oci.oraclecloud.com/agentendpoint/YOUR_AGENT_ID/chat"
    
    # Load OCI configuration
    config = load_oci_config("/Workspace/oci_config.ini")
    
    # Create client
    client = AIDPChatClient(aidp_url, config, auth="api_key")
    
    print("=" * 60)
    print("Batch Processing Example")
    print("=" * 60)
    
    # List of queries to process
    queries = [
        "What is Python?",
        "What is machine learning?",
        "What is cloud computing?",
        "What is data science?",
        "What is artificial intelligence?"
    ]
    
    results = []
    
    print(f"\nProcessing {len(queries)} queries...\n")
    
    for i, query in enumerate(queries, 1):
        print(f"[{i}/{len(queries)}] Processing: {query}")
        
        try:
            # Send query
            response = client.chat(query, stream=False)
            
            # Extract text content
            content = extract_text_content(response)
            
            # Store result
            results.append({
                "query": query,
                "response": content,
                "success": True
            })
            
            print(f"✅ Success - Response length: {len(content)} characters")
            
            # Small delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            results.append({
                "query": query,
                "error": str(e),
                "success": False
            })
    
    # Print summary
    print("\n" + "=" * 60)
    print("Batch Processing Results")
    print("=" * 60)
    
    successful = sum(1 for r in results if r["success"])
    failed = len(results) - successful
    
    print(f"\nTotal queries: {len(queries)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    
    # Print detailed results
    print("\n--- Detailed Results ---\n")
    for i, result in enumerate(results, 1):
        print(f"{i}. Query: {result['query']}")
        if result["success"]:
            print(f"   Response: {result['response'][:100]}...")
        else:
            print(f"   Error: {result['error']}")
        print()
    
    print("=" * 60)
    print("✅ Batch processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
