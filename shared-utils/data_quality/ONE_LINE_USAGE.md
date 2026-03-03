# 🎯 ONE LINE USAGE - SIMPLEST WAY!

## The Absolute Simplest Way to Run Data Quality Checks

### Option 1: Import and Run (ONE LINE!)

```python
from dq_checker import check_data_quality
check_data_quality()
```

**That's it! Literally one line of code!**

### Option 2: Even Simpler - Just Run the File

```bash
python dq_checker.py
```

**Done! No code to write at all!**

---

## What Happens Automatically?

When you call `check_data_quality()`:

1. ✅ **Generates sample data** (users and orders tables)
2. ✅ **Runs ALL quality checks** (null, unique, range, pattern, foreign keys, etc.)
3. ✅ **Shows beautiful results** in console
4. ✅ **Returns results** as dictionary

**No configuration needed. No arguments needed. Just run it!**

### 🤖 GenAI Support
✅ **Includes AI-powered semantic validation** using OCI GenAI!  
✅ Automatically checks data quality using LLM models  
✅ See `GENAI_USAGE.md` for details on AI checks

---

## Usage Examples

### In a Python Script
```python
from dq_checker import check_data_quality

# That's all you need!
results = check_data_quality()

# Optional: Check the pass rate
print(f"Pass Rate: {results['pass_rate']}%")
```

### In AIDP Notebook
```python
# Cell 1: Run DQ checks
from dq_checker import check_data_quality
results = check_data_quality()

# Cell 2: Analyze results (optional)
if results['pass_rate'] < 100:
    print(f"Found {results['checks_failed']} issues")
```

### In CI/CD Pipeline
```python
from dq_checker import check_data_quality

results = check_data_quality()

# Fail pipeline if quality is bad
if results['pass_rate'] < 95:
    raise Exception("Data quality below threshold!")
```

---

## What You Get

### Console Output:
```
📊 RESULTS SUMMARY
Total Checks: 17
✅ Passed: 13
❌ Failed: 4
🎯 Pass Rate: 76.47%
```

### Results Dictionary:
```python
{
    'success': True,
    'checks_run': 17,
    'checks_passed': 13,
    'checks_failed': 4,
    'pass_rate': 76.47,
    'details': [...]  # Full details of each check
}
```

---

## Advanced Usage (Optional)

If you need more control, you can use the underlying function:

```python
from run_all_checks import run_all_dq_checks

# With custom config
results = run_all_dq_checks(
    config_path="my_config.yaml",
    generate_sample_data=False,  # Use existing data
    save_results=True            # Save to CSV
)
```

But for most cases, just use:
```python
from dq_checker import check_data_quality
check_data_quality()
```

---

## FAQ

**Q: Do I need to write a config file?**  
A: No! Default config is already there.

**Q: Do I need to generate data?**  
A: No! It auto-generates sample data.

**Q: What checks does it run?**  
A: Everything! Null checks, unique, range, pattern, foreign keys, etc.

**Q: Can I disable some checks?**  
A: Yes, edit the config file and set `enabled: false` for any check.

**Q: How do I use my own data?**  
A: Put your config in `config/` folder and call:
```python
from run_all_checks import run_all_dq_checks
run_all_dq_checks(config_path="config/my_config.yaml")
```

---

## Comparison

### ❌ Old Way (Too much code!)
```python
import sys
sys.path.append("...")
from pyspark.sql import SparkSession
spark = SparkSession.builder...
df = spark.read.csv(...)
df.write.saveAsTable(...)
from jobs.run_dq_job import run_dq_checks
config = load_config(...)
results = run_dq_checks(...)
results.show()
```

### ✅ New Way (ONE LINE!)
```python
from dq_checker import check_data_quality
check_data_quality()
```

---

## Summary

**You literally need ONE line of code:**

```python
from dq_checker import check_data_quality; check_data_quality()
```

**Or even simpler, just run:**
```bash
python dq_checker.py
```

**No configuration. No setup. No arguments. Just works!** 🎉

---

For detailed documentation, see `README.md` and `QUICK_START.md`.
