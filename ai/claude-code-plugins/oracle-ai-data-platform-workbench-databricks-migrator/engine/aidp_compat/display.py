"""
AIDP display() replacement for Databricks display()
=====================================================
Databricks display() provides rich DataFrame visualization.
This provides compatible alternatives for AIDP/Jupyter.
"""


def display(obj, *args, **kwargs):
    """Drop-in replacement for Databricks display() function.

    Handles:
    - PySpark DataFrames -> df.show() or df.toPandas() in Jupyter
    - Pandas DataFrames -> IPython display
    - Matplotlib figures -> IPython display
    - Lists/dicts -> print
    """
    # Try IPython display first (works in Jupyter notebooks)
    try:
        from IPython.display import display as ipy_display

        # PySpark DataFrame
        if hasattr(obj, 'toPandas') and hasattr(obj, 'show'):
            try:
                # If small enough, convert to pandas for rich display
                count = obj.count()
                if count <= 1000:
                    ipy_display(obj.toPandas())
                else:
                    # Too large for pandas, use show()
                    obj.show(100, truncate=False)
            except Exception:
                obj.show(100, truncate=False)
            return

        # Pandas DataFrame
        if hasattr(obj, 'to_html') and hasattr(obj, 'describe'):
            ipy_display(obj)
            return

        # Matplotlib figure
        if hasattr(obj, 'savefig'):
            ipy_display(obj)
            return

        # StreamingQuery
        if hasattr(obj, 'status') and hasattr(obj, 'awaitTermination'):
            print(f"StreamingQuery: {obj.status}")
            return

        # Default: IPython display
        ipy_display(obj)
        return

    except ImportError:
        pass

    # Fallback: print-based display
    if hasattr(obj, 'show'):
        obj.show(100, truncate=False)
    elif hasattr(obj, 'to_string'):
        print(obj.to_string())
    else:
        print(obj)


def displayHTML(html):
    """Display HTML content (Databricks displayHTML replacement)."""
    try:
        from IPython.display import display as ipy_display, HTML
        ipy_display(HTML(html))
    except ImportError:
        print("[HTML content - not rendered in non-Jupyter environment]")
        print(html[:500])
