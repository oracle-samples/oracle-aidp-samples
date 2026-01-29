# OCI Document Q&A Pipeline

A production-ready PySpark application that implements Retrieval-Augmented Generation (RAG) for question-answering over documents stored in Oracle Cloud Infrastructure (OCI) Object Storage buckets.

## Overview

This pipeline enables intelligent question-answering over your document collections by:
1. Reading documents from OCI Object Storage
2. Chunking documents into manageable pieces
3. Creating semantic embeddings for each chunk
4. Retrieving relevant context using similarity search
5. Generating answers using an LLM with retrieved context

## Features

- **Cloud-Native**: Direct integration with OCI Object Storage
- **Scalable Processing**: Built on PySpark for distributed document processing
- **Smart Chunking**: Configurable chunking with overlap for context preservation
- **Semantic Search**: Vector embeddings with cosine similarity matching
- **RAG Architecture**: Combines retrieval with generation for accurate answers
- **Safe Generation**: Built-in prompt truncation and character limits
- **Source Attribution**: Tracks and cites source documents for answers
- **Error Handling**: Robust parsing with graceful degradation

## Architecture

```
OCI Bucket Documents
    ↓
Binary File Reading (PySpark)
    ↓
Text Extraction & Truncation
    ↓
Chunking (1200 chars, 200 overlap)
    ↓
Embedding Generation (LLM)
    ↓
[Question] → Query Embedding
    ↓
Cosine Similarity Ranking
    ↓
Top-K Retrieval
    ↓
Context Building (with budget)
    ↓
LLM Generation with Context
    ↓
Answer + Citations
```

## Configuration

### Basic Setup

```python
# OCI Bucket Configuration
BUCKET = "test_doc"
NAMESPACE = "idseylbmv0mm"
PREFIX = "documents/"

# Model Configuration
GENERATION_MODEL = "default.oci_ai_models.xai.grok-4"
EMBEDDING_MODEL = "default.oci_ai_models.xai.grok-4"

# Processing Parameters
CHUNK_SIZE = 1200           # Characters per chunk
CHUNK_OVERLAP = 200         # Overlap between chunks
TOP_K = 8                   # Number of chunks to retrieve
MAX_FILE_CHARS = 200_000    # Max chars per file
MAX_PROMPT_CHARS = 10_000   # Max chars in context
```

### Supported File Types

Currently supports:
- `.txt` - Plain text files
- `.md` - Markdown files

Can be extended to support:
- `.pdf` - PDF documents
- `.docx` - Word documents
- `.html` - HTML files

## Quick Start

### Basic Usage

```python
from oci_doc_qa import ask

# Ask a question about your documents
ask("What are the key features of the product?")

# More examples
ask("Summarize the security guidelines")
ask("What are the deployment requirements?")
ask("Explain the pricing model")
```

### Programmatic Usage

```python
from oci_doc_qa import answer_question

# Get answer and sources
answer, source_paths = answer_question("What is the refund policy?")

print("Answer:", answer)
print("Sources:", source_paths)
```

## Core Components

### 1. Document Loading

```python
binary_df = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(BASE_URI)
    .filter(col("ext").isin(*SUPPORTED_TEXT_EXT))
)
```

Reads all supported files from the specified OCI bucket path recursively.

### 2. Text Extraction

```python
def parse_binary_to_text(path: str, ext: str, content: bytes) -> str
```

Converts binary file content to UTF-8 text with:
- Automatic truncation for large files
- Error handling for encoding issues
- Parse error reporting

### 3. Chunking Strategy

```python
def split_into_chunks(text: str, chunk_size: int, overlap: int)
```

**Why Chunking?**
- LLMs have token limits
- Smaller chunks improve retrieval precision
- Overlap preserves context across boundaries

**Configuration:**
- `CHUNK_SIZE=1200`: Balance between context and granularity
- `CHUNK_OVERLAP=200`: Prevents information loss at boundaries

### 4. Embedding Generation

```python
def embed_texts(texts: List[str]) -> List[List[float]]
```

Generates dense vector representations for semantic similarity:
- Batch processing for efficiency
- Automatic format normalization
- Error handling for malformed embeddings

### 5. Retrieval System

```python
def retrieve(question: str, k: int = TOP_K) -> List[Tuple[str, str, float]]
```

Implements semantic search:
1. Embeds the question
2. Computes cosine similarity with all chunks
3. Returns top-K most relevant chunks with scores

**Cosine Similarity Formula:**
```
similarity = (A · B) / (||A|| × ||B||)
```

### 6. Context Building

```python
def _build_context_under_budget(scored_ctx, max_chars: int) -> str
```

Intelligently constructs context:
- Respects character budget (10,000 chars)
- Preserves source attribution
- Truncates oversized chunks
- Prioritizes highest-scoring content

### 7. Answer Generation

```python
def answer_question(question: str) -> Tuple[str, List[str]]
```

Generates answers with:
- Retrieved context injection
- Source citation
- Prompt truncation for safety
- Factual grounding

## Example Workflow

### Step-by-Step Execution

```python
# 1. Load documents
binary_df = spark.read.format("binaryFile").load(BASE_URI)
# Output: Found 25 supported files

# 2. Parse to text
text_df = binary_df.withColumn("content", parse_binary_to_text(...))

# 3. Create chunks
chunks_df = text_df.withColumn("chunks", chunk_udf(col("content")))
# Output: Total chunks: 342

# 4. Generate embeddings
CHUNK_EMB = embed_texts(CHUNK_TEXTS)
# Output: Got 342 chunk embeddings

# 5. Ask question
ask("What are the deployment requirements?")
```

### Example Output

```
===== ANSWER =====

The deployment requirements include:

• **Infrastructure**: Kubernetes cluster v1.24+ with at least 3 nodes
• **Resources**: 8GB RAM minimum per node, 50GB storage
• **Dependencies**: PostgreSQL 13+, Redis 6.2+
• **Network**: Load balancer with SSL/TLS support
• **Monitoring**: Prometheus and Grafana for observability

[Source 1] oci://test_doc@idseylbmv0mm/documents/deployment-guide.md
[Source 2] oci://test_doc@idseylbmv0mm/documents/infrastructure-specs.txt

===== SOURCES =====

oci://test_doc@idseylbmv0mm/documents/deployment-guide.md
oci://test_doc@idseylbmv0mm/documents/infrastructure-specs.txt
```

## Advanced Configuration

### Adjusting Retrieval Quality

```python
# More context (higher recall, more tokens)
TOP_K = 12
MAX_PROMPT_CHARS = 15_000

# Less context (faster, lower cost)
TOP_K = 5
MAX_PROMPT_CHARS = 7_000
```

### Optimizing Chunk Size

```python
# Smaller chunks (better precision, more chunks)
CHUNK_SIZE = 800
CHUNK_OVERLAP = 150

# Larger chunks (more context per chunk, fewer chunks)
CHUNK_SIZE = 1600
CHUNK_OVERLAP = 300
```

### Custom Prompt Template

```python
def _make_prompt(context_text: str, question: str) -> str:
    return f"""
You are a technical documentation expert.

RULES:
- Answer only using the provided context
- Use technical terminology accurately
- Provide code examples when relevant
- Format responses in markdown

CONTEXT:
{context_text}

QUESTION:
{question}

ANSWER (with citations):
""".strip()
```

## Performance Optimization

### Memory Management

```python
# Limit chunks processed in-memory
chunk_rows = chunks_df.select(...).limit(200).collect()
```

For production with large document sets:

```python
# Process in batches
def process_in_batches(chunk_df, batch_size=500):
    total = chunk_df.count()
    for offset in range(0, total, batch_size):
        batch = chunk_df.limit(batch_size).offset(offset)
        # Process batch...
```

### Caching Embeddings

```python
# Save embeddings to avoid recomputation
embeddings_df = spark.createDataFrame(
    [(id, path, emb) for id, path, emb in zip(CHUNK_IDS, CHUNK_PATHS, CHUNK_EMB)],
    ["chunk_id", "path", "embedding"]
)
embeddings_df.write.parquet("oci://bucket/embeddings/")

# Load cached embeddings
cached_emb = spark.read.parquet("oci://bucket/embeddings/")
```

### Parallel Embedding Generation

```python
# Use Spark's parallelism for large batches
def embed_texts_parallel(texts: List[str], batch_size=100):
    batches = [texts[i:i+batch_size] for i in range(0, len(texts), batch_size)]
    df = spark.createDataFrame([(i, batch) for i, batch in enumerate(batches)], 
                               ["batch_id", "batch"])
    # Process in parallel...
```

## Troubleshooting

### Issue: "No relevant content found"

**Causes:**
- Empty bucket or wrong path
- File format not supported
- All files failed to parse

**Solutions:**
```python
# Check file count
print(f"Files found: {binary_df.count()}")

# Check supported extensions
text_df.select("path", "ext").show()

# Verify bucket path
print(f"Looking in: {BASE_URI}")
```

### Issue: "Embedding dimension mismatch"

**Cause:** Inconsistent embedding models or formats

**Solution:**
```python
# Verify embedding format
sample_emb = embed_texts(["test"])
print(f"Embedding dimension: {len(sample_emb[0])}")

# Ensure consistent model
assert GENERATION_MODEL == EMBEDDING_MODEL
```

### Issue: "Prompt too long" errors

**Causes:**
- Context exceeds model limits
- Large chunks
- High TOP_K

**Solutions:**
```python
# Reduce context budget
MAX_PROMPT_CHARS = 8_000

# Reduce retrieval count
TOP_K = 5

# Smaller chunks
CHUNK_SIZE = 1000
```

### Issue: Poor answer quality

**Possible causes:**
1. Irrelevant chunks retrieved
2. Question-document mismatch
3. Insufficient context

**Debugging:**
```python
# Check retrieved chunks
def debug_retrieve(question: str):
    top = retrieve(question, TOP_K)
    for i, (path, text, score) in enumerate(top, 1):
        print(f"\n[{i}] Score: {score:.3f}")
        print(f"Path: {path}")
        print(f"Text: {text[:200]}...")

debug_retrieve("your question")
```

## Extending the Pipeline

### Adding PDF Support

```python
def parse_pdf(content: bytes) -> str:
    import PyPDF2
    import io
    
    try:
        pdf = PyPDF2.PdfReader(io.BytesIO(content))
        text = ""
        for page in pdf.pages:
            text += page.extract_text()
        return _safe_truncate(text)
    except Exception as e:
        return f"[PDF_PARSE_ERROR]: {e}"

# Update supported extensions
SUPPORTED_TEXT_EXT = {".txt", ".md", ".pdf"}
```

### Adding Metadata Filtering

```python
def retrieve_with_filters(question: str, filters: dict, k: int = TOP_K):
    """
    filters = {"department": "engineering", "date_after": "2024-01-01"}
    """
    q_embs = embed_texts([question])
    q_emb = q_embs[0]
    
    # Filter chunks by metadata
    filtered_chunks = [
        (path, text, vec) 
        for path, text, vec in zip(CHUNK_PATHS, CHUNK_TEXTS, CHUNK_EMB)
        if matches_filters(path, filters)
    ]
    
    # Score and rank
    scored = [
        (path, text, cosine_sim(q_emb, ensure_vector(vec)))
        for path, text, vec in filtered_chunks
    ]
    scored.sort(key=lambda x: x[2], reverse=True)
    return scored[:k]
```

### Adding Reranking

```python
def rerank_results(question: str, chunks: List[Tuple], model: str):
    """Use a cross-encoder for more accurate ranking."""
    rerank_df = spark.createDataFrame([
        (i, question, text) 
        for i, (path, text, score) in enumerate(chunks)
    ], ["idx", "question", "text"])
    
    # Use reranking model
    reranked = rerank_df.select(
        col("idx"),
        expr(f"query_model('{model}', concat(question, '|', text)) as score")
    )
    
    # Resort by new scores
    # ... implementation
```

## Best Practices

### Document Preparation

1. **Clean Documents**: Remove boilerplate, headers, footers
2. **Consistent Formatting**: Use markdown for structured content
3. **Metadata**: Include creation dates, authors in filenames
4. **Size Limits**: Split very large documents (>200KB)

### Query Optimization

1. **Specific Questions**: "What is the API rate limit?" vs "Tell me about APIs"
2. **Use Keywords**: Include domain-specific terms
3. **Context Hints**: "According to the security docs..." 
4. **Iterate**: Refine questions based on initial results

### Production Deployment

1. **Monitor Costs**: Track LLM API usage
2. **Cache Aggressively**: Store embeddings, frequent queries
3. **Error Handling**: Log failures, implement retries
4. **Access Control**: Secure bucket access, audit queries
5. **Version Documents**: Track document updates for cache invalidation

## Cost Considerations

### Embedding Costs

```python
# One-time cost per document update
num_chunks = 342
cost_per_embedding = 0.0001  # Example rate
total_embedding_cost = num_chunks * cost_per_embedding
# = $0.0342 per document set
```

### Generation Costs

```python
# Per-query cost
avg_prompt_tokens = 2500
cost_per_query = 0.001  # Example rate
# = $0.001 per question
```

### Optimization Strategies

1. **Cache embeddings** - Only recompute when documents change
2. **Batch queries** - Process multiple questions together
3. **Tune TOP_K** - Balance accuracy vs. context size
4. **Smart truncation** - Remove redundant content

## Use Cases

- **Enterprise Knowledge Base**: Query company documentation
- **Customer Support**: Automated response from help docs
- **Research Assistant**: Find relevant passages in papers
- **Legal Document Analysis**: Extract clauses and terms
- **Technical Documentation**: Navigate API docs and guides
- **Compliance Checking**: Verify policy adherence

## Limitations

1. **Text-Only**: Currently no image/table extraction from PDFs
2. **English-Centric**: Embeddings may work poorly for other languages
3. **No Cross-Document Reasoning**: Each chunk treated independently
4. **Static Content**: No real-time web search or updates
5. **Memory Constraints**: Large document sets need careful batch management

## Security Considerations

- **Data Privacy**: Documents remain in your OCI bucket
- **Access Control**: Use OCI IAM policies
- **Audit Logging**: Track all queries and accessed documents
- **PII Detection**: Consider scanning for sensitive data
- **Encryption**: Use OCI encryption at rest and in transit
