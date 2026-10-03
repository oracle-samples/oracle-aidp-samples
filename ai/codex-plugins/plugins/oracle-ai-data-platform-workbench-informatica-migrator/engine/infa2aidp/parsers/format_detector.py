"""Auto-detect Informatica mapping format (PowerCenter XML vs IICS JSON).

Dispatches to the correct parser based on the first non-whitespace character.

Reference: the Rust reference implementation, src/parser/mod.rs
"""

import logging

from ..models import MigrationResult

logger = logging.getLogger(__name__)


def detect_and_parse(content: str) -> MigrationResult:
    """Auto-detect format and parse.

    Args:
        content: Raw file content (XML or JSON string)

    Returns:
        MigrationResult from the appropriate parser

    Raises:
        ValueError: If format cannot be detected
    """
    trimmed = content.lstrip()

    if trimmed.startswith("{") or trimmed.startswith("["):
        # IICS Cloud JSON
        logger.info("Detected IICS Cloud JSON format")
        from .iics_parser import IICSParser
        parser = IICSParser()
        return parser.parse(content)

    elif trimmed.startswith("<"):
        # PowerCenter XML — should not reach here, use detect_and_parse_file instead
        raise ValueError(
            "Use detect_and_parse_file() for XML files. "
            "XML parsing requires the file-based parser path."
        )

    else:
        raise ValueError(
            "Cannot detect Informatica mapping format. "
            "Input must start with '<' (PowerCenter XML) or '{' (IICS Cloud JSON)."
        )


def detect_and_parse_file(path: str) -> MigrationResult:
    """Auto-detect format and parse a file.

    Uses the same proven file-based path as the main branch for XML.
    Only JSON goes through the string-based parser.

    Args:
        path: Path to XML or JSON file

    Returns:
        MigrationResult from the appropriate parser
    """
    # Detect format from file extension first, then content
    lower_path = path.lower()

    if lower_path.endswith(".json"):
        # IICS Cloud JSON
        logger.info("Detected IICS Cloud JSON format: %s", path)
        from .iics_parser import IICSParser
        parser = IICSParser()
        return parser.parse_file(path)

    elif lower_path.endswith(".xml"):
        # PowerCenter XML — use the same file-based parser as main branch
        logger.info("Detected PowerCenter XML format: %s", path)
        from .xml_parser import InformaticaXMLParser
        parser = InformaticaXMLParser()
        return parser.parse(path)

    else:
        # Unknown extension — peek at content. utf-8-sig drops a UTF-8 BOM
        # (common on exports saved from Windows tools), which otherwise
        # arrived as U+FEFF and made every BOM-prefixed file "undetectable".
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            first_char = f.read(64).strip()[:1]

        if first_char == "{" or first_char == "[":
            from .iics_parser import IICSParser
            return IICSParser().parse_file(path)
        elif first_char == "<":
            from .xml_parser import InformaticaXMLParser
            return InformaticaXMLParser().parse(path)
        else:
            raise ValueError(
                f"Cannot detect format for {path}. "
                "File must be .xml (PowerCenter) or .json (IICS Cloud)."
            )
