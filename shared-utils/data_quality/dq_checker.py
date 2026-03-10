"""
🎯 SIMPLEST DATA QUALITY CHECKER - ONE LINE DOES EVERYTHING!

Just import and run - that's it!

Example:
    from dq_checker import check_data_quality
    check_data_quality()  # That's all!
"""

def check_data_quality():
    """
    ✨ ONE FUNCTION - RUNS EVERYTHING!
    
    NO arguments needed. Just call it.
    
    This function will:
    - ✅ Generate sample data automatically
    - ✅ Load all tables
    - ✅ Run all data quality checks
    - ✅ Display beautiful results
    - ✅ Return results dictionary
    
    Example:
        from dq_checker import check_data_quality
        results = check_data_quality()
    
    That's literally it! One line!
    """
    from run_all_checks import run_all_dq_checks
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║                                                          ║
    ║    🚀 DATA QUALITY CHECKER - RUNNING ALL CHECKS         ║
    ║                                                          ║
    ║    No configuration needed - using smart defaults!      ║
    ║                                                          ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Run with best defaults
    results = run_all_dq_checks(
        generate_sample_data=True,  # Auto-generate test data
        output_format="console",     # Nice console output
        save_results=False           # Don't clutter with files
    )
    
    return results


# Make it even simpler - you can just run this file!
if __name__ == "__main__":
    check_data_quality()
