# Prompt Templates - Complete Overview

## 📁 Files Included

| File | Purpose | Templates |
|------|---------|-----------|
| `prompt_templates.json` | **Default** - Basic data analysis (ACTIVE) | 15 beginner-friendly |
| `prompt_templates.advanced.json` | Advanced ML/AI workflows | 15 expert-level |
| `prompt_templates.example.json` | Sample custom templates | 4 examples |

## 🎯 Quick Start

### For Beginners (Current Setup)
✅ You're ready to go! The app is using basic templates perfect for:
- Learning data concepts
- Exploring datasets
- Basic SQL queries
- Data cleaning
- Simple visualizations
- Everyday analysis tasks

**👉 See [GETTING_STARTED.md](GETTING_STARTED.md) for step-by-step examples!**

### For Advanced Users
Want ML, feature engineering, and production templates?

```bash
# Swap to advanced templates
cp prompt_templates.advanced.json prompt_templates.json

# Reload in app (click "🔄 Reload" button in sidebar)
```

## 📚 Documentation

| Document | What's Inside |
|----------|---------------|
| **[GETTING_STARTED.md](GETTING_STARTED.md)** | Complete guide with examples for all 15 basic templates |
| **[TEMPLATE_QUICK_REFERENCE.md](TEMPLATE_QUICK_REFERENCE.md)** | Quick lookup: which template for which task |
| **[TEMPLATES.md](TEMPLATES.md)** | Technical docs for customization |
| **[README.md](README.md)** | App setup and deployment |

## 🔄 Switching Between Template Sets

### Use Basic Templates (Default)
```bash
# Already active! No action needed.
# Or restore if you switched:
git checkout prompt_templates.json
```

### Use Advanced Templates
```bash
cp prompt_templates.advanced.json prompt_templates.json
# Click "🔄 Reload" in sidebar
```

### Use Both (Create Custom Mix)
```bash
# Edit prompt_templates.json manually
# Combine templates from both files
# Add your own custom templates
```

## 🎓 Learning Path

### Week 1-2: Master the Basics
Use `prompt_templates.json` (current):
- Start with "Getting Started" template
- Work through "Explore Dataset"
- Try "Basic SQL Query"
- Practice "Clean Data"

### Week 3-4: Level Up
When comfortable with basics, add advanced templates:
- "Feature Engineering"
- "Debug Model Performance"
- "Optimize Pipeline"

### Week 5+: Customize
Create your own templates for your specific workflows!

## 🛠️ Customization

### Add Your Own Template

Edit `prompt_templates.json`:
```json
{
  "My Custom Template": {
    "template": "Your prompt with {variables}",
    "description": "What it does"
  }
}
```

Click "🔄 Reload" in the sidebar.

### Template Variables
Use `{variable_name}` for user inputs:
- `{data}` - Data sample
- `{task}` - Task description
- `{columns}` - Column names
- `{metric}` - Metric to calculate

## 💡 Examples by Use Case

### Data Exploration
```
Templates: Explore Dataset → Simple Data Summary → Find Patterns
```

### SQL Queries
```
Templates: Basic SQL Query → Join Tables → Filter Data
```

### Data Cleaning
```
Templates: Explore Dataset → Clean Data → Handle Missing Data
```

### Visualization
```
Templates: Visualize Data → Compare Groups → Interpret Results
```

### Prediction
```
Templates: Find Patterns → Predict Outcome → Interpret Results
```

## 🐳 Docker Usage

### Mount Custom Templates
```bash
docker run -v ./prompt_templates.json:/app/prompt_templates.json \
           -p 8501:8501 your-image
```

### Switch Templates in Container
```bash
# Use advanced templates
docker run -v ./prompt_templates.advanced.json:/app/prompt_templates.json \
           -p 8501:8501 your-image
```

See [docker-compose.example.yml](docker-compose.example.yml) for complete examples.

## 📊 Template Comparison

| Feature | Basic (15) | Advanced (15) |
|---------|-----------|---------------|
| **Target Users** | Beginners, analysts | ML engineers, data scientists |
| **Complexity** | Simple, educational | Technical, production-focused |
| **Focus** | Learning & exploration | Optimization & deployment |
| **Variables** | 1-3 per template | 3-5 per template |
| **Examples** | SQL, visualization, cleaning | ML, pipelines, A/B tests |

## ❓ FAQ

**Q: Which template set should I use?**
A: Start with basic (current default). Switch to advanced when you need ML/production features.

**Q: Can I use both?**
A: Yes! Edit `prompt_templates.json` and include templates from both files.

**Q: How do I create custom templates?**
A: Copy the format from existing templates and add to `prompt_templates.json`.

**Q: Will my changes persist?**
A: Yes, if saved to the JSON file. In-app additions are session-only.

**Q: Can I share templates with my team?**
A: Yes! Commit your `prompt_templates.json` to version control.

## 🚀 Next Steps

1. **Try the basics** - Use 3-5 templates from the current set
2. **Read the guide** - Check out [GETTING_STARTED.md](GETTING_STARTED.md)
3. **Customize** - Add templates for your specific use cases
4. **Level up** - Switch to advanced when ready
5. **Share** - Help teammates with your custom templates

---

**Need help?** Check the documentation files or ask using the "Data Question" template!
