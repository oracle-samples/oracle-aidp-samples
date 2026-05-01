"""Oracle AI Data Platform Fusion Bundle.

Productized Fusion ERP/HCM/SCM -> AIDP pipeline:
- Curated BICC extracts (`extractors.bicc`) mirroring the official Oracle sample
- Fallback Fusion REST paged extracts (`extractors.rest`, 499-row hard cap per MOS 2429019.1)
- Saas-batch REST (`extractors.saas_batch_rest`) for v1.5+ targeted JSON extraction
- Bronze/silver/gold medallion in Delta (`transforms.*`)
- Conformed dimensions (`dimensions.*`)
- OAC dashboard install via REST API (`oac.*`); end users consume via OAC MCP (Preview)

Public CLI: `aidp-fusion-bundle` (see `cli.py`).

Reference plan: C:/Users/anuma/.claude/plans/oracle-ai-data-platform-fusion-bundle.md
"""

from __future__ import annotations

__version__ = "0.1.0a0"
__all__ = ["__version__"]
