# 🤖 GenAI-Powered Data Quality Checks

## Yes! The Tool Supports AI-Powered Semantic Validation

The data quality tool includes **AI-powered semantic checks** using OCI GenAI models in addition to traditional rule-based checks.

## 🎯 Two Types of Checks

### 1. Traditional Rules (Fast, No Cost)
- NOT NULL
- UNIQUE
- RANGE
- PATTERN (regex)
- DUPLICATE
- COMPLETENESS
- TYPE CHECK
- VALUE SET
- FOREIGN KEY

### 2. AI-Powered Rules (Smart, Uses GenAI)
- **LLM SEMANTIC** - AI validates data based on natural language descriptions
- Uses OCI GenAI models (Llama, Cohere, etc.)
- Perfect for complex validation that's hard to express as rules

## 📝 How GenAI Checks Work

### Config Example:
```yaml
rules:
  - name: email_semantic_validation
    type: llm_semantic
    column: email
    description: "valid professional email addresses"
    model: meta.llama-3.1-70b-instruct  # OCI GenAI model
    sample_size: 30                      # Samples to check
    severity: MEDIUM
    weight: 5
```

### What Happens:
1. Tool samples 30 email values from your data
2. Sends them to GenAI with the description
3. AI validates if they match "valid professional email addresses"
4. Returns PASS/FAIL

## 🚀 Usage Examples

### Example 1: Email Validation
```yaml
- name: check_professional_emails
  type: llm_semantic
  column: email
  description: "corporate email addresses from known companies"
  model: meta.llama-3.1-70b-instruct
```

### Example 2: Product Names
```yaml
- name: check_product_names
  type: llm_semantic
  column: product_name
  description: "proper product names without typos or gibberish"
  model: meta.llama-3.1-70b-instruct
```

### Example 3: Addresses
```yaml
- name: check_addresses
  type: llm_semantic
  column: address
  description: "complete valid US addresses with street, city, state, zip"
  model: meta.llama-3.1-70b-instruct
```

### Example 4: Comments/Reviews
```yaml
- name: check_review_quality
  type: llm_semantic
  column: review_text
  description: "meaningful customer reviews, not spam or gibberish"
  model: meta.llama-3.1-70b-instruct
  sample_size: 50
```

## 🎛️ Enable/Disable GenAI Checks

GenAI checks can be expensive (API costs), so you can easily disable them:

```yaml
# Disable GenAI check
- name: email_semantic_validation
  type: llm_semantic
  column: email
  description: "valid professional email addresses"
  enabled: false  # ❌ Disabled - no cost!
```

## 💡 Best Practices

### When to Use GenAI Checks:
✅ Complex semantic validation (e.g., "Is this a real address?")  
✅ Natural language rules (e.g., "professional tone")  
✅ Pattern recognition hard to express in regex  
✅ Quality of text content (reviews, comments)  
✅ Contextual validation (e.g., "appropriate for business")

### When NOT to Use GenAI Checks:
❌ Simple null checks → Use `not_null`  
❌ Numeric ranges → Use `range`  
❌ Simple patterns → Use `pattern` (regex)  
❌ Uniqueness → Use `unique`  
❌ Budget constraints → Disable LLM checks

## 🔧 Available AI Models

The tool supports OCI GenAI models:

```yaml
# Meta Llama
model: meta.llama-3.1-70b-instruct
model: meta.llama-3.1-405b-instruct

# Cohere
model: cohere.command-r-plus

# OpenAI (via OCI)
model: openai.gpt-4

# Custom models
model: your.custom.model
```

## 💰 Cost Considerations

GenAI checks use API calls, which have costs:

**Optimization Tips:**
1. Use `sample_size` to limit samples (default: 30)
2. Disable LLM checks in dev/test (enable in production only)
3. Use traditional rules where possible
4. Set `enabled: false` for expensive checks

```yaml
# Cost-optimized config
- name: ai_check
  type: llm_semantic
  sample_size: 20      # Fewer samples = lower cost
  enabled: false       # Only enable when needed
```

## 📊 Example: Full Config with GenAI

```yaml
tables:
  products:
    table_name: default.products
    rules:
      # Traditional checks (fast, free)
      - name: product_id_not_null
        type: not_null
        column: product_id
        severity: CRITICAL
        weight: 10
      
      - name: price_range
        type: range
        column: price
        min: 0
        max: 10000
        severity: HIGH
        weight: 7
      
      # GenAI check (smart, costs money)
      - name: description_quality
        type: llm_semantic
        column: description
        description: "clear, professional product descriptions without typos"
        model: meta.llama-3.1-70b-instruct
        sample_size: 25
        severity: MEDIUM
        weight: 5
        enabled: true  # Enable only when needed
```

## 🎯 Running with GenAI

### Same ONE LINE Usage!
```python
from dq_checker import check_data_quality

# Runs ALL checks including GenAI
results = check_data_quality()
```

### Enable/Disable via Config
Just set `enabled: true/false` in YAML - no code changes needed!

## 🔍 How to Check if GenAI is Working

The tool will show in output:
```
Running llm_semantic check on email...
   Using model: meta.llama-3.1-70b-instruct
   Sampled 30 values
   ✓ PASS
```

## 📈 Comparison

### Without GenAI:
```
✅ email matches pattern: PASS  
❓ But is it a REAL professional email?
```

### With GenAI:
```
✅ email matches pattern: PASS
✅ email is professional and corporate: PASS
🎉 High confidence it's valid!
```

## Summary

- ✅ **GenAI is INCLUDED and WORKING**
- ✅ Uses OCI GenAI models (Llama, Cohere, etc.)
- ✅ Perfect for semantic/contextual validation
- ✅ Easy to enable/disable per check
- ✅ Same simple one-line usage!

```python
from dq_checker import check_data_quality
check_data_quality()  # Includes GenAI checks!
```

---

**Note:** GenAI checks require OCI GenAI access in your AIDP environment. If not available, they'll be skipped automatically.
