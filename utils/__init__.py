"""
Utilities for merge operations on PySpark DataFrames.

This package provides tools for partition-aware merge operations
with configurable update behaviors, similar to Delta Lake MERGE operations.
"""

from utils.merge_generator import get_merged_df

__all__ = ['get_merged_df']
