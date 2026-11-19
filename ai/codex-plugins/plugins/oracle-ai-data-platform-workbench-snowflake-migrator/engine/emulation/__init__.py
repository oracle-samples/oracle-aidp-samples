"""Dev-mode emulation: a synthetic Snowflake estate and an emulated AIDP.

Nothing in this package opens a socket. The demo pipeline runs the SAME
extractors, planner, DDL generator and deploy code as a real run — only the
two transports (`run_sql` for Snowflake, `call` for AIDP) are replaced by the
fakes here, exactly the seam the unit tests use.
"""
