"""IICS (Informatica Intelligent Cloud Services) JSON Parser.

Parses IICS Cloud mapping definitions (JSON format) into the same
internal data model used by the PowerCenter XML parser. This enables
the same conversion pipeline for both on-premises and cloud Informatica.

IICS JSON schema:
{
  "name": "mapping_name",
  "description": "...",
  "transformations": [
    {"name": "...", "type": "SOURCE|FILTER|...", "fields": [...], ...}
  ],
  "connections": [
    {"from": {"transformation": "...", "field": "..."}, "to": {...}}
  ]
}

Reference: the Rust reference implementation, src/parser/iics.rs
"""

import json
import logging
from typing import Optional

from ..models import (
    Connector,
    ConnectionInfo,
    DataFlowDirection,
    FieldMapping,
    InfaVersion,
    LoadStrategy,
    Mapping,
    MigrationResult,
    Session,
    SourceDefinition,
    TargetDefinition,
    Transformation,
    TransformationField,
    TransformationType,
    Workflow,
)
from ..properties import aggregator_group_by_fallback
from .security import validate_input_size, validate_no_xxe

logger = logging.getLogger(__name__)

# Max input size: 10 MB
_MAX_INPUT_SIZE = 10 * 1024 * 1024

# IICS transform type → internal TransformationType
from ..transformation_types import resolve as _resolve_type

_TYPE_MAP = {
    "source": TransformationType.SOURCE_QUALIFIER,
    "source qualifier": TransformationType.SOURCE_QUALIFIER,
    "sourcequalifier": TransformationType.SOURCE_QUALIFIER,
    "filter": TransformationType.FILTER,
    "expression": TransformationType.EXPRESSION,
    "joiner": TransformationType.JOINER,
    "lookup": TransformationType.LOOKUP,
    "aggregator": TransformationType.AGGREGATOR,
    "router": TransformationType.ROUTER,
    "sorter": TransformationType.SORTER,
    "rank": TransformationType.RANK,
    "sequence generator": TransformationType.SEQUENCE_GENERATOR,
    "sequencegenerator": TransformationType.SEQUENCE_GENERATOR,
    "update strategy": TransformationType.UPDATE_STRATEGY,
    "updatestrategy": TransformationType.UPDATE_STRATEGY,
    "union": TransformationType.UNION,
    "normalizer": TransformationType.NORMALIZER,
    "stored procedure": TransformationType.STORED_PROCEDURE,
    "storedprocedure": TransformationType.STORED_PROCEDURE,
    "custom": TransformationType.CUSTOM,
    "java": TransformationType.JAVA,
    "sql": TransformationType.SQL,
    "sql transformation": TransformationType.SQL,
    "http": TransformationType.HTTP,
    "transaction control": TransformationType.TRANSACTION_CONTROL,
    "transactioncontrol": TransformationType.TRANSACTION_CONTROL,
    "xml parser": TransformationType.XML_PARSER,
    "xml generator": TransformationType.XML_GENERATOR,
    "mapplet": TransformationType.MAPPLET,
    "target": TransformationType.UNKNOWN,  # Handled separately
}

# IICS port direction → internal DataFlowDirection.
#
# "variable"/"var" map to DataFlowDirection.VARIABLE, not INPUT_OUTPUT
# (a gap left by, which added VARIABLE and wired
# the PowerCenter XML path to it, including detection of a self-referencing
# variable port that needs a window function, but left this IICS map
# collapsing every variable port into an ordinary INPUT_OUTPUT column. IICS
# is the primary source format, so a self-referencing variable port
# (v_run = v_run + AMOUNT) here silently became a plain withColumn instead
# of the explicit review item the XML path already produces -- wrong for a
# running total, and no error to say so.)
#
# Also accepts the IN/OUT/INOUT/VAR short forms and normalizes a "-" to "_"
# before lookup (matches the upstream Rust parser's
# `.replace('-', '_')` in iics.rs:186-192).
def _is_truthy(value) -> bool:
    """Loose boolean coercion for IICS JSON flags that may arrive as a
    real JSON boolean OR as the string "true"/"false" -- ``bool("false")``
    is ``True`` in Python, so a plain truthiness check would silently treat
    an explicit "master": "false" as master.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("true", "yes", "1")


_DIR_MAP = {
    "input": DataFlowDirection.INPUT,
    "in": DataFlowDirection.INPUT,
    "output": DataFlowDirection.OUTPUT,
    "out": DataFlowDirection.OUTPUT,
    "input_output": DataFlowDirection.INPUT_OUTPUT,
    "input/output": DataFlowDirection.INPUT_OUTPUT,
    "inout": DataFlowDirection.INPUT_OUTPUT,
    "variable": DataFlowDirection.VARIABLE,
    "var": DataFlowDirection.VARIABLE,
}


class IICSParser:
    """Parse IICS Cloud JSON mapping definitions.

    Produces the same MigrationResult as InformaticaXMLParser,
    allowing the same downstream pipeline for both formats.
    """

    def parse(self, json_input: str) -> MigrationResult:
        """Parse an IICS Cloud JSON mapping.

        Args:
            json_input: Raw JSON string (mapping definition)

        Returns:
            MigrationResult with mappings, sources, targets, etc.
        """
        # Security checks
        validate_input_size(json_input, _MAX_INPUT_SIZE)

        data = json.loads(json_input)
        result = MigrationResult()
        result.version = InfaVersion.V10  # IICS is always 10.x+
        result.version_detail = "IICS Cloud"

        mapping = self._parse_mapping(data)
        result.mappings.append(mapping)

        return result

    def parse_file(self, path: str) -> MigrationResult:
        """Parse an IICS JSON file."""
        with open(path, "r", encoding="utf-8") as f:
            return self.parse(f.read())

    def _parse_mapping(self, data: dict) -> Mapping:
        """Parse the top-level mapping object."""
        name = data.get("name") or data.get("mappingName", "unknown")
        # IDMC/IICS assets are organized by project/folder in the Cloud
        # repository; carry that through for output foldering.
        folder = data.get("project") or data.get("folder") or ""
        mapping = Mapping(
            name=name,
            description=data.get("description", ""),
            folder=folder,
        )

        # Parse transformations
        sources = []
        targets = []
        for tx_data in data.get("transformations", []):
            # Primary ∥ fallback key, matching upstream (the Rust reference implementation,
            # iics.rs:61-63) -- an export that only sets "transformationType"
            # used to fall through as "" here and never get routed to the
            # source/target special-casing below.
            tx_type = (tx_data.get("type") or tx_data.get("transformationType") or "").lower().strip()

            if tx_type in ("source", "source definition"):
                src = self._parse_source(tx_data)
                sources.append(src)
                # Also create a Source Qualifier transformation
                sq = self._parse_transformation(tx_data)
                sq.type = TransformationType.SOURCE_QUALIFIER
                mapping.transformations.append(sq)
            elif tx_type in ("target", "target definition"):
                tgt = self._parse_target(tx_data)
                targets.append(tgt)
            else:
                tx = self._parse_transformation(tx_data)
                mapping.transformations.append(tx)

        mapping.sources = sources
        mapping.targets = targets

        # Parse connections. Primary ∥ fallback key (matches
        # upstream iics.rs:47-49) -- an export using "connectors" instead of
        # "connections" used to silently lose the whole data-flow graph.
        for conn_data in (data.get("connections") or data.get("connectors") or []):
            conn = self._parse_connection(conn_data)
            if conn:
                mapping.connectors.append(conn)

        return mapping

    def _parse_transformation(self, data: dict) -> Transformation:
        """Parse a single transformation."""
        name = data.get("name", "")
        # Primary ∥ fallback key (matches upstream
        # iics.rs:61-63) -- "type" only used to mean an export keying the
        # transform kind under "transformationType" instead came back
        # TransformationType.UNKNOWN with no error.
        raw_type_verbatim = (data.get("type") or data.get("transformationType") or "")
        raw_type = raw_type_verbatim.lower().strip()
        # _TYPE_MAP first (this parser's established vocabulary), then the
        # shared cross-platform resolver, which knows the IDMC-native types
        # PowerCenter has no word for -- Hierarchy Processor, Structure
        # Parser, Cleanse and the rest. Before this, any CDI-native
        # transformation collapsed to UNKNOWN and was reported as
        # "Unsupported transformation type: Unknown", naming nothing.
        tx_type = _TYPE_MAP.get(raw_type) or _resolve_type(raw_type_verbatim)

        tx = Transformation(name=name, type=tx_type, raw_type=raw_type_verbatim)
        tx.description = data.get("description", "")

        # Parse fields/ports. Primary ∥ fallback key (
        # matches upstream iics.rs:69) -- "ports" only used to silently
        # drop every field on the transformation.
        for field_data in (data.get("fields") or data.get("ports") or []):
            tf = self._parse_field(field_data)
            tx.fields.append(tf)

        # Type-specific attributes
        tx.filter_condition = data.get("filterCondition") or data.get("condition", "")
        tx.join_condition = data.get("joinCondition", "")
        tx.join_type = data.get("joinType", "")
        tx.sql_override = data.get("sqlOverride") or data.get("sqlQuery", "")
        tx.lookup_table = data.get("tableName", "")
        # IDMC exports spell a Lookup's condition either lookupCondition or,
        # like a Joiner, joinCondition. Only the first was read, so a Lookup
        # carrying joinCondition compiled to a condition-less join -- a
        # cross join on AIDP (seen 2026-09-25 on the star-schema fixture).
        tx.lookup_condition = data.get("lookupCondition") or (
            data.get("joinCondition", "") if "LOOKUP" in raw_type.upper() else ""
        )
        tx.lookup_sql = data.get("lookupSqlOverride", "")
        tx.update_strategy_expression = data.get("updateStrategyExpression", "")

        # Connection reference. Primary ∥ fallback key (
        # matches upstream iics.rs:167-169) -- filed under the canonical
        # "connection_ref" property key regardless of which spelling the
        # export used, instead of leaving a caller to guess which raw JSON
        # key the generic properties bucket below happened to preserve.
        connection_ref = data.get("connectionName") or data.get("connection") or ""
        if connection_ref:
            tx.properties["connection_ref"] = connection_ref

        # Group by fields (Aggregator).
        # Priority 1: an explicit transform-level "groupByFields" list.
        tx.group_by_fields = data.get("groupByFields", [])
        if not tx.group_by_fields and tx_type == TransformationType.AGGREGATOR:
            # Priority 2: an explicit per-field groupBy/isGroupBy flag (set
            # on tx.fields by _parse_field above) -- if ANY field carries
            # it, honour those flagged fields exactly and stop.
            flagged = [f.name for f in tx.fields if f.is_group_by]
            if flagged:
                tx.group_by_fields = flagged
            else:
                # Priority 3 (fallback heuristic, only reached when NO field
                # is explicitly flagged): same non-aggregated-Input-port
                # heuristic the XML parser uses -- see
                # properties.aggregator_group_by_fallback. Fixes the same
                # defect for the IICS format: a bare "portType":
                # "INPUT" field on an Aggregator with no other marker used
                # to silently collapse a per-group aggregation into one
                # global row.
                tx.group_by_fields = aggregator_group_by_fallback(tx.fields)

        # Sort keys (Sorter)
        for sk in data.get("sortKeys", []):
            if isinstance(sk, dict):
                tx.sort_keys.append({
                    "field": sk.get("fieldName") or sk.get("name", ""),
                    "direction": sk.get("direction", "ASC"),
                })
            elif isinstance(sk, str):
                tx.sort_keys.append(sk)
        tx.sort_direction = data.get("sortDirection", "ASC")

        # Router groups. Primary ∥ fallback key for the group list itself
        # (matches upstream iics.rs:85) -- "groups" only
        # used to silently lose every router branch. Same for each group's
        # condition (matches upstream iics.rs:89-92): "filterCondition"
        # only used to leave that branch's condition "", which the
        # generator then treats as an extra, ungoverned default group.
        for rg in (data.get("routerGroups") or data.get("groups") or []):
            tx.router_groups.append({
                "name": rg.get("name", ""),
                "condition": rg.get("condition") or rg.get("filterCondition") or "",
            })

        # Sequence Generator
        if tx_type == TransformationType.SEQUENCE_GENERATOR:
            tx.start_value = data.get("startValue", 1)
            tx.increment_by = data.get("incrementBy", 1)

        # Store all properties
        for key in data:
            if key not in ("name", "type", "fields", "connections", "description"):
                tx.properties[key] = data[key]

        return tx

    def _parse_field(self, data: dict) -> TransformationField:
        """Parse a single field/port."""
        # A missing portType/direction key must default to INPUT_OUTPUT, not
        # INPUT. INPUT_OUTPUT is the safe dataclass default on
        # TransformationField.direction (models.py) and is also the evident
        # intent of the map-miss fallback just below -- but that fallback
        # never fired for a missing key, because the old "INPUT" default
        # matched _DIR_MAP successfully. Defaulting to INPUT instead silently
        # dropped every expression on a field whose IICS export omitted
        # portType, since transformation_converter.py only emits a field's
        # expression when direction is OUTPUT/INPUT_OUTPUT (e.g. _expression,
        # _aggregator, _lookup, _union, _stored_procedure,
        # _sql_transformation, _normalizer, _mapplet) -- the notebook still
        # generated and exited 0, just with the transformation logic gone.
        # Normalize "-" to "_" (matches upstream iics.rs:186)
        # so a hyphenated spelling like "input-output" still matches
        # _DIR_MAP's "input_output" key.
        raw_dir = (
            data.get("portType") or data.get("direction") or "input_output"
        ).lower().strip().replace("-", "_")
        direction = _DIR_MAP.get(raw_dir, DataFlowDirection.INPUT_OUTPUT)

        expression = data.get("expression", "")
        if expression and direction == DataFlowDirection.INPUT:
            # Belt and braces: a field carrying a non-empty expression is by
            # definition an output (or output-producing) port -- an
            # input-only port cannot have a computed expression. Treat it as
            # INPUT_OUTPUT even if some IICS export explicitly marks it
            # INPUT, so a mismarked source export can't reintroduce the same
            # silent data loss the default above just fixed.
            direction = DataFlowDirection.INPUT_OUTPUT

        # A Joiner's master-vs-detail split is a per-field marker in IICS
        # too: a boolean "master"/"isMaster" flag, or a "portGroup"/"group"
        # of "master" (matches the Rust reference implementation,
        # src/ast.rs Port::is_master). Values may arrive as JSON booleans
        # or as string "true"/"false", so compare loosely rather than
        # relying on Python truthiness of the raw value (the string
        # "false" is truthy).
        port_group = str(data.get("portGroup") or data.get("group") or "").strip().lower()
        is_master = (
            _is_truthy(data.get("master"))
            or _is_truthy(data.get("isMaster"))
            or port_group == "master"
        )

        # IICS marks an Aggregator's grouping ports with a per-field
        # "groupBy" (some exports use "isGroupBy") boolean -- same shape as
        # the "master"/"isMaster" flag above, and the same reason it must be
        # read explicitly rather than inferred from direction (
        # matches the Rust reference implementation, src/ast.rs Port::is_group_by /
        # src/parser/iics.rs's per-field groupBy read).
        is_group_by = _is_truthy(data.get("groupBy")) or _is_truthy(data.get("isGroupBy"))

        return TransformationField(
            name=data.get("name", ""),
            datatype=data.get("dataType") or data.get("datatype") or data.get("type") or "STRING",
            precision=data.get("precision", 0) or 0,
            scale=data.get("scale", 0) or 0,
            expression=expression,
            direction=direction,
            default_value=data.get("defaultValue", ""),
            description=data.get("description", ""),
            is_master=is_master,
            is_group_by=is_group_by,
        )

    def _parse_source(self, data: dict) -> SourceDefinition:
        """Parse a source transformation into a SourceDefinition."""
        table_name = data.get("tableName") or data.get("name", "")
        # Parse 3-part name: catalog.schema.table
        parts = table_name.split(".")
        src = SourceDefinition(name=data.get("name", table_name))
        if len(parts) >= 3:
            src.db_name = parts[0]
            src.owner = parts[1]
            src.table_name = parts[2]
        elif len(parts) == 2:
            src.owner = parts[0]
            src.table_name = parts[1]
        else:
            src.table_name = table_name

        src.sql_query = data.get("sqlOverride") or data.get("sqlQuery", "")

        # Parse fields as FieldMappings
        for field_data in data.get("fields", []):
            fm = FieldMapping(
                source_field=field_data.get("name", ""),
                target_field=field_data.get("name", ""),
                datatype=field_data.get("dataType") or field_data.get("datatype", "STRING"),
                precision=field_data.get("precision", 0) or 0,
                scale=field_data.get("scale", 0) or 0,
            )
            src.fields.append(fm)

        return src

    def _parse_target(self, data: dict) -> TargetDefinition:
        """Parse a target transformation into a TargetDefinition."""
        table_name = data.get("tableName") or data.get("name", "")
        parts = table_name.split(".")
        tgt = TargetDefinition(name=data.get("name", table_name))
        if len(parts) >= 3:
            tgt.db_name = parts[0]
            tgt.owner = parts[1]
            tgt.table_name = parts[2]
        elif len(parts) == 2:
            tgt.owner = parts[0]
            tgt.table_name = parts[1]
        else:
            tgt.table_name = table_name

        # Load strategy
        strategy = (data.get("loadStrategy") or data.get("writeMode", "")).upper()
        strategy_map = {
            "INSERT": LoadStrategy.INSERT,
            "UPDATE": LoadStrategy.UPDATE,
            "UPSERT": LoadStrategy.UPSERT,
            "DELETE": LoadStrategy.DELETE,
            "SCD_TYPE1": LoadStrategy.SCD_TYPE1,
            "SCD_TYPE2": LoadStrategy.SCD_TYPE2,
            "TRUNCATE_INSERT": LoadStrategy.TRUNCATE_INSERT,
        }
        tgt.load_strategy = strategy_map.get(strategy, LoadStrategy.INSERT)

        # Parse fields
        for field_data in data.get("fields", []):
            is_key = field_data.get("isKey", False) or field_data.get("keyType", "") != ""
            fm = FieldMapping(
                source_field=field_data.get("name", ""),
                target_field=field_data.get("name", ""),
                datatype=field_data.get("dataType") or field_data.get("datatype", "STRING"),
                precision=field_data.get("precision", 0) or 0,
                scale=field_data.get("scale", 0) or 0,
                is_key=is_key,
            )
            tgt.fields.append(fm)

        return tgt

    def _parse_connection(self, data: dict) -> Optional[Connector]:
        """Parse a connection (data flow link).

        Supports both nested and flat formats:
        Nested: {"from": {"transformation": "A", "field": "X"}, "to": {...}}
        Flat:   {"fromTransformation": "A", "fromField": "X", "toTransformation": "B", "toField": "Y"}
        """
        if "from" in data and isinstance(data["from"], dict):
            # Nested format
            from_tx = data["from"].get("transformation", "")
            from_field = data["from"].get("field", "")
            to_tx = data["to"].get("transformation", "")
            to_field = data["to"].get("field", "")
        else:
            # Flat format
            from_tx = data.get("fromTransformation", "")
            from_field = data.get("fromField", "")
            to_tx = data.get("toTransformation", "")
            to_field = data.get("toField", "")

        if from_tx and to_tx:
            return Connector(
                from_instance=from_tx,
                from_field=from_field,
                to_instance=to_tx,
                to_field=to_field,
            )
        return None
