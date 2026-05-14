def truncate_output(text, max_lines=200):
    if not text:
        return ""
    try:
        max_lines = int(max_lines)
    except (TypeError, ValueError):
        max_lines = 200
    lines = text.strip().split("\n")
    if len(lines) > max_lines:
        lines = lines[:max_lines] + [f"... ({len(lines) - max_lines} lines truncated)"]
    return "\n".join(lines)

def sanitize_path(base_dir, relative_path):
    import os
    if not relative_path:
        return base_dir
    base = os.path.normpath(base_dir)
    full = os.path.normpath(os.path.join(base, relative_path))
    if full != base and not full.startswith(base + os.sep):
        return None
    return full
