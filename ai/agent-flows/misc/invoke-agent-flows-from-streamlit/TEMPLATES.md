# Prompt Templates Configuration

This application supports customizable prompt templates loaded from a JSON file.

## File Location

The app looks for `prompt_templates.json` in the following locations (in order):

1. Current working directory: `./prompt_templates.json`
2. Same directory as the script: `<app_dir>/prompt_templates.json`
3. Docker mount location: `/app/prompt_templates.json`
4. Custom path via environment variable: `$TEMPLATES_PATH/prompt_templates.json`

## File Format

### Basic Format (Simple)
```json
{
  "Template Name": "Template text with {variable} placeholders"
}
```

### Extended Format (With Descriptions)
```json
{
  "Template Name": {
    "template": "Template text with {variable} placeholders",
    "description": "Optional description of what this template does"
  }
}
```

## Template Variables

Use `{variable_name}` syntax for placeholders that users will fill in. Examples:

- `{task}` - A task description
- `{code}` - Code snippet
- `{schema}` - Database schema
- `{data}` - Data sample

When a template is selected, the app will automatically detect variables and create input fields for each one.

## Example Template

```json
{
  "Generate SQL Query": {
    "template": "Generate a SQL query to {task}.\n\nTable schema:\n{schema}\n\nRequirements:\n{requirements}",
    "description": "Create SQL queries based on requirements and schema"
  }
}
```

## Customization

### For Standalone Deployment

1. Edit `prompt_templates.json` in the app directory
2. Restart the Streamlit app
3. Your custom templates will be loaded automatically

### For Docker Deployment

**Option 1: Mount a custom templates file**
```bash
docker run -v /path/to/your/prompt_templates.json:/app/prompt_templates.json \
           -p 8501:8501 your-image-name
```

**Option 2: Use environment variable for custom path**
```bash
docker run -e TEMPLATES_PATH=/custom/path \
           -v /path/to/templates:/custom/path \
           -p 8501:8501 your-image-name
```

**Option 3: Build with custom templates**
```dockerfile
FROM your-base-image
COPY my_custom_templates.json /app/prompt_templates.json
```

## Default Templates

If no template file is found, the app uses built-in default templates for AI Data Platform use cases:

- Generate SQL Query
- Explain Model Results
- Data Quality Check
- Feature Engineering
- And more...

## Adding Templates at Runtime

You can also add templates through the UI:

1. Open the sidebar
2. Expand "➕ Add New Template"
3. Enter template name and text
4. Click "💾 Save"

**Note:** Templates added through the UI are stored in session state and will be lost when the app restarts. To persist them, add them to `prompt_templates.json`.

## Template Best Practices

1. **Clear variable names**: Use descriptive names like `{model_type}` instead of `{x}`
2. **Provide context**: Include enough context in the template so the AI understands the task
3. **Structure**: Use line breaks and formatting to make templates readable
4. **Variables**: Keep the number of variables reasonable (3-5 is ideal)
5. **Descriptions**: Add descriptions to help users understand when to use each template

## Troubleshooting

- **Templates not loading**: Check console output for error messages
- **File not found**: Verify the file path and ensure `prompt_templates.json` exists
- **Invalid JSON**: Validate your JSON syntax at jsonlint.com
- **Missing templates**: The app will fall back to defaults if the file is invalid

## Reloading Templates

To reload templates from the file:

1. Edit `prompt_templates.json`
2. Click "Refresh" in the sidebar (if available)
3. Or restart the Streamlit app

Changes to the template file require an app restart to take effect.
