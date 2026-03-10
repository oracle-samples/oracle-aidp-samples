# 🚀 Quick Start - One Function Does Everything!

## Sabse Simple Way - Ek Function Call!

```python
from run_all_checks import run_all_dq_checks

# Bas itna hi! Sab checks ho jayenge
results = run_all_dq_checks(generate_sample_data=True)
```

## ✨ Usage Examples

### 1. Quick Test (Generate Data + Run Checks)
```python
from run_all_checks import quick_test

# Ek line mein sab kuch!
results = quick_test()
# ✅ Data generate hoga
# ✅ Sabhi checks run honge
# ✅ Results console mein dikhenge
```

### 2. Custom Configuration ke Saath
```python
from run_all_checks import run_all_dq_checks

results = run_all_dq_checks(
    config_path="config/my_custom_config.yaml",
    generate_sample_data=False,
    output_format="all",  # console + json dono
    save_results=True     # CSV file mein save karega
)
```

### 3. Production Run
```python
from run_all_checks import production_run

# Production ke liye ready - results save honge
results = production_run("config/production_dq_config.yaml")
```

### 4. AIDP Workbench mein
```python
# Notebook mein ek cell mein run karo
from run_all_checks import run_all_dq_checks

results = run_all_dq_checks(
    config_path="/Workspace/oracle-aidp-samples/data_quality/config/demo_dq_config.yaml"
)

# Results check karo
print(f"Pass Rate: {results['pass_rate']}%")
print(f"Failed: {results['checks_failed']} checks")
```

## 🎛️ Checks Enable/Disable Kaise Karein

Configuration file mein `enabled: false` set karo:

```yaml
tables:
  users:
    rules:
      # Yeh check run hoga
      - name: email_not_null
        type: not_null
        column: email
        enabled: true
      
      # Yeh check SKIP hoga
      - name: email_pattern
        type: pattern
        column: email
        pattern: '^[a-zA-Z0-9._%+-]+@.*$'
        enabled: false  # ❌ Disabled
      
      # Expensive AI check - disable kar diya
      - name: llm_semantic_check
        type: llm_semantic
        column: email
        description: "valid emails"
        enabled: false  # 💰 Paisa bachega!
```

## 📊 Results Structure

```python
results = {
    'success': True,
    'checks_run': 12,
    'checks_passed': 12,
    'checks_failed': 0,
    'pass_rate': 100.0,
    'details': [
        {
            'table': 'users',
            'rule': 'email_not_null',
            'type': 'not_null',
            'status': 'PASS',
            'severity': 'CRITICAL',
            'weight': 10
        },
        # ... more results
    ]
}
```

## 🎯 Use Cases

### Development: Quick Test
```python
# Local testing - data generate karke test karo
from run_all_checks import quick_test
quick_test()
```

### Testing: Custom Config
```python
# Testing environment ke liye
results = run_all_dq_checks(
    config_path="config/test_config.yaml",
    save_results=True
)
```

### Production: Full Run
```python
# Production data par run karo
from run_all_checks import production_run
results = production_run("config/production_config.yaml")

# Alert system integrate karo
if results['pass_rate'] < 95:
    send_alert(f"DQ Failed: {results['checks_failed']} issues")
```

### CI/CD Pipeline
```python
# Automated testing
results = run_all_dq_checks(generate_sample_data=True)
if results['pass_rate'] < 100:
    exit(1)  # Pipeline fail ho jayegi
```

## 🔧 Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `config_path` | str | demo config | Path to YAML config |
| `generate_sample_data` | bool | False | Generate test data |
| `output_format` | str | "console" | "console", "json", "dataframe", or "all" |
| `save_results` | bool | False | Save to CSV file |

## 📝 Config File Options

### Check Level
```yaml
- name: check_name
  type: not_null
  column: col_name
  enabled: true        # true/false
  severity: CRITICAL   # CRITICAL/HIGH/MEDIUM/LOW
  weight: 10          # For scoring
```

### Table Level
```yaml
users:
  enabled: true       # Entire table enable/disable
  table_name: default.users
  rules: [...]
```

### Cross-Table Level
```yaml
cross_table_rules:
  - name: fk_check
    type: foreign_key
    enabled: true     # Enable/disable FK check
    source: {...}
    target: {...}
```

## 💡 Pro Tips

1. **Development**: Enable sab checks
2. **Testing**: Disable expensive LLM checks
3. **Production**: Enable critical checks only
4. **Debugging**: `enabled: false` karke specific check isolate karo

## 🎨 Output Formats

### Console (Default)
```
📊 RESULTS SUMMARY
Total Checks: 12
✅ Passed: 12
❌ Failed: 0
🎯 Pass Rate: 100.00%
```

### JSON
```json
{
  "checks_run": 12,
  "checks_passed": 12,
  "checks_failed": 0,
  "pass_rate": 100.0,
  "details": [...]
}
```

### DataFrame (Spark)
```python
results['dataframe'].show()
# +-------+--------+------+--------+----------+
# | table | rule   | type | status | severity |
# +-------+--------+------+--------+----------+
# | users | id_chk | ...  | PASS   | CRITICAL |
# +-------+--------+------+--------+----------+
```

## 🚨 Common Issues

### Issue: Module Not Found
```python
import sys
sys.path.append("/Workspace/oracle-aidp-samples/data_quality")
from run_all_checks import run_all_dq_checks
```

### Issue: Table Not Found
```python
# Pehle table create karo
spark.read.csv("data.csv").write.saveAsTable("default.users")
# Phir DQ run karo
results = run_all_dq_checks()
```

### Issue: Config Not Found
```python
# Full path do
results = run_all_dq_checks(
    config_path="/full/path/to/config.yaml"
)
```

## ✅ That's It!

Ek function call - sab kuch ho gaya! 🎉

```python
from run_all_checks import run_all_dq_checks
results = run_all_dq_checks(generate_sample_data=True)
```

---

**Need help?** Check `README.md` for detailed documentation.
