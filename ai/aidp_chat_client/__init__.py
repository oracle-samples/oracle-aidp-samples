"""
AIDP Chat Client - A Python library for interacting with Oracle AIDP Chat Agent endpoints.
"""

from .client import AIDPChatClient
from .utils import get_last_response_id, print_response_content, get_traces

__version__ = "1.0.0"
__all__ = [
    "AIDPChatClient",
    "get_last_response_id",
    "print_response_content",
    "get_traces",
]
