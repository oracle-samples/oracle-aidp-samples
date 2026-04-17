# Getting Started with Data Analysis Prompts

This guide helps you use the basic data analysis templates to get started with your data projects.

---

## 📚 Template Categories

### 🌱 For Beginners

These templates help you learn data analysis concepts and get started with your first projects.

#### **1. Getting Started**
**Use when:** You want to learn a new data concept
**Example:**
- Topic: "What is a correlation coefficient?"
- Topic: "How do I choose between mean and median?"

#### **2. Explore Dataset**
**Use when:** You have a new dataset and don't know where to start
**Example:**
- Dataset name: "customer_sales"
- Columns: "customer_id, date, product, price, quantity"

**What you'll get:**
- Key metrics to calculate first
- Important columns to analyze
- Potential data quality issues to check

#### **3. Simple Data Summary**
**Use when:** You want quick insights from a small dataset
**Example:**
```
Data:
Day 1: 100 sales
Day 2: 150 sales
Day 3: 120 sales
Day 4: 180 sales
Day 5: 200 sales
```

**What you'll get:**
- Average, trends, and patterns
- Interesting observations
- Next steps for analysis

---

### 📊 Basic Analysis

#### **4. Basic SQL Query**
**Use when:** You need to query a database
**Example:**
- Task: "find top 10 customers by total revenue"
- Table: "orders"
- Columns: "customer_id, order_date, total_amount"

**What you'll get:**
- Working SQL query
- Explanation of each part
- Tips for optimization

#### **5. Visualize Data**
**Use when:** You want to create a chart but don't know which type
**Example:**
- Data description: "Monthly sales for 12 months"
- Or: "Comparing 5 products by customer rating"

**What you'll get:**
- Best chart type (line, bar, pie, etc.)
- How to prepare the data
- What insights the chart will show

#### **6. Find Patterns**
**Use when:** You want to discover relationships in data
**Example:**
```
Data:
Temperature: 70F, Ice cream sales: $500
Temperature: 80F, Ice cream sales: $750
Temperature: 90F, Ice cream sales: $1000
```

**What you'll get:**
- Correlations and relationships
- Unexpected patterns
- Recommendations for deeper analysis

---

### 🧹 Data Cleaning

#### **7. Clean Data**
**Use when:** Your data has errors or inconsistencies
**Example:**
- Data sample: "John Smith, john.smith@email, 2024-13-45, $1,000.00"
- Issues: "Invalid dates, mixed formats, missing values"

**What you'll get:**
- Step-by-step cleaning instructions
- Code or formulas to use
- How to verify the results

#### **8. Handle Missing Data**
**Use when:** You have blank or null values
**Example:**
- Columns: "age, income, purchase_amount"
- Dataset size: "1000 rows"
- Missing percentage: "15% missing in income column"

**What you'll get:**
- Should you delete, fill, or impute?
- Best method for your situation
- Impact on your analysis

---

### 🔍 Common Tasks

#### **9. Compare Groups**
**Use when:** You want to compare two segments
**Example:**
- Group 1: "Customers who received email campaign"
- Group 2: "Customers who didn't receive email"
- Metric: "Average purchase amount"

**What you'll get:**
- Difference between groups
- Is it significant?
- What it means for your business

#### **10. Calculate Metrics**
**Use when:** You need to compute a specific metric
**Example:**
- Metric: "customer retention rate"
- Data: "1000 customers last month, 850 returned this month"

**What you'll get:**
- Formula breakdown
- Calculation result
- Interpretation of the value

#### **11. Join Tables**
**Use when:** You need to combine data from multiple sources
**Example:**
- Table 1: "customers" with "customer_id, name, email"
- Table 2: "orders" with "order_id, customer_id, amount"

**What you'll get:**
- Type of join to use (inner, left, etc.)
- SQL or code to combine them
- What to watch out for

#### **12. Filter Data**
**Use when:** You want to find specific records
**Example:**
- Criteria: "customers who spent more than $1000 last month"
- Dataset: "customer_transactions"
- Columns: "customer_id, transaction_date, amount"

**What you'll get:**
- Filter logic (SQL WHERE, pandas query, etc.)
- How to handle edge cases
- Count of matching records

---

### 🔮 Making Predictions

#### **13. Predict Outcome**
**Use when:** You want to forecast or predict something
**Example:**
- Outcome: "next month's sales"
- Features: "historical sales, seasonality, marketing spend"

**What you'll get:**
- Simple approach to start with
- What data you need
- How to validate predictions

---

### 💡 Understanding Results

#### **14. Interpret Results**
**Use when:** You have analysis output but don't understand it
**Example:**
```
Results:
R-squared: 0.85
P-value: 0.003
Coefficient: 2.5
```

**What you'll get:**
- Plain language explanation
- What each number means
- Whether the results are good or bad

#### **15. Data Question**
**Use when:** You have any other question about your data
**Example:**
- Question: "Why is my average higher than my median?"
- Context: "Sales data for 100 transactions"

**What you'll get:**
- Direct answer to your question
- Related concepts to understand
- Suggestions for next steps

---

## 🎯 Quick Start Guide

### Step 1: Choose Your Template
1. Open the sidebar
2. Go to "📝 Prompt Templates"
3. Select the template that matches your task

### Step 2: Fill in Variables
- The app will show input fields for each variable
- Fill them with your specific data or requirements
- Be as specific as possible for better results

### Step 3: Send and Learn
- Click "📤 Use Template"
- Click "📤 Send" to submit
- Read the response and ask follow-up questions

---

## 💡 Tips for Best Results

### Be Specific
❌ Bad: "Data: some numbers"
✅ Good: "Data: Daily website visitors for January 2024: 100, 150, 120, 180..."

### Provide Context
❌ Bad: "Analyze this"
✅ Good: "Analyze this sales data to understand seasonal patterns"

### Include Units
❌ Bad: "Revenue: 50, 75, 100"
✅ Good: "Revenue: $50k, $75k, $100k per month"

### Specify Format
❌ Bad: "Show me how"
✅ Good: "Show me SQL code for PostgreSQL"

### Ask Follow-ups
- If something is unclear, ask for clarification
- Request examples or more detail
- Ask for alternative approaches

---

## 📖 Example Workflows

### Workflow 1: Exploring New Data
1. **Explore Dataset** - Understand what you have
2. **Simple Data Summary** - Get initial insights
3. **Find Patterns** - Look for relationships
4. **Visualize Data** - Create charts
5. **Data Question** - Ask specific questions

### Workflow 2: Cleaning Messy Data
1. **Explore Dataset** - See what's there
2. **Clean Data** - Fix errors and inconsistencies
3. **Handle Missing Data** - Deal with nulls
4. **Simple Data Summary** - Verify improvements

### Workflow 3: Comparing Performance
1. **Compare Groups** - See differences
2. **Calculate Metrics** - Compute KPIs
3. **Visualize Data** - Chart the comparison
4. **Interpret Results** - Understand what it means

### Workflow 4: Making Predictions
1. **Explore Dataset** - Understand your features
2. **Find Patterns** - Identify correlations
3. **Predict Outcome** - Build simple model
4. **Interpret Results** - Validate predictions

---

## 🚀 Next Steps

### Once You're Comfortable
- Try the **advanced templates** (prompt_templates.advanced.json)
- Create your own **custom templates**
- Combine multiple templates for complex workflows

### For Advanced Users
Copy `prompt_templates.advanced.json` to `prompt_templates.json` to access:
- Feature engineering
- Model optimization
- Production deployment
- Cost optimization
- And more...

### Learning Resources
- Experiment with small datasets first
- Ask "Getting Started" questions about concepts
- Use "Data Question" for anything unclear
- Build up from simple to complex analysis

---

## ❓ Need Help?

### Common Questions

**Q: Which template should I use?**
A: Start with "Explore Dataset" if you're new to the data, or "Data Question" if you have a specific question.

**Q: What if I don't know what to put in a variable?**
A: Describe what you have in plain language. Example: "I don't know my exact schema" - just describe the columns you remember.

**Q: Can I modify templates?**
A: Yes! Edit `prompt_templates.json` to customize any template or create new ones.

**Q: What if the response isn't helpful?**
A: Try being more specific, provide more context, or rephrase as a follow-up question.

---

## 🎓 Learning Path

### Week 1: Basics
- Explore Dataset
- Simple Data Summary
- Basic SQL Query
- Visualize Data

### Week 2: Cleaning
- Clean Data
- Handle Missing Data
- Filter Data
- Join Tables

### Week 3: Analysis
- Find Patterns
- Compare Groups
- Calculate Metrics
- Interpret Results

### Week 4: Prediction
- Predict Outcome
- Interpret Results
- Create custom workflows

---

Happy analyzing! 🎉
