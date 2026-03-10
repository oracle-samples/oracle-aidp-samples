"""
volume_checks.py
------------------------------------------------------------
Responsibility:
- Detect sudden data volume spikes or drops
- Protect SLAs and downstream analytics
------------------------------------------------------------
"""

def check_volume(curr_count: int, prev_count: int, warn_pct: int, fail_pct: int):
    """
    Compare current vs previous record counts.

    Returns:
        tuple: (STATUS, percentage_difference)
    """
    diff_pct = abs(curr_count - prev_count) * 100 / prev_count

    if diff_pct > fail_pct:
        return "FAIL", diff_pct
    elif diff_pct > warn_pct:
        return "WARN", diff_pct
    return "PASS", diff_pct
