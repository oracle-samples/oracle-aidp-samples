"""Data models for Informatica to AIDP migration.

Defines the intermediate representation (IR) used between parsing
Informatica XML and generating AIDP notebooks.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class InfaVersion(Enum):
    V8 = "8.x"
    V9 = "9.x"
    # 10.x below 10.5 -- 10.4 and earlier. Detected and named so the tool can
    # refuse precisely, but out of scope: 10.4 left standard support in March
    # 2024. Kept as a distinct member rather than folded into V10_5 because
    # "we support 10.5" and "we support 10.x" are different promises, and the
    # gate should enforce the one the README makes.
    V10 = "10.x"
    V10_5 = "10.5.x"
    UNKNOWN = "unknown"


class TransformationType(Enum):
    SOURCE_QUALIFIER = "Source Qualifier"
    EXPRESSION = "Expression"
    FILTER = "Filter"
    JOINER = "Joiner"
    LOOKUP = "Lookup"
    AGGREGATOR = "Aggregator"
    ROUTER = "Router"
    SEQUENCE_GENERATOR = "Sequence Generator"
    UPDATE_STRATEGY = "Update Strategy"
    STORED_PROCEDURE = "Stored Procedure"
    NORMALIZER = "Normalizer"
    RANK = "Rank"
    SORTER = "Sorter"
    UNION = "Union"
    CUSTOM = "Custom Transformation"
    JAVA = "Java Transformation"
    SQL = "SQL Transformation"
    HTTP = "HTTP Transformation"
    TRANSACTION_CONTROL = "Transaction Control"
    XML_PARSER = "XML Parser"
    XML_GENERATOR = "XML Generator"
    MAPPLET = "Mapplet"

    # ---- Source/target as first-class instances.
    # PowerCenter calls these Source Definition / Target Definition; IDMC
    # calls them Source / Target and treats them as transformations on the
    # canvas. Distinct from SOURCE_QUALIFIER, which is the PowerCenter read
    # operator and has no IDMC equivalent.
    SOURCE = "Source"
    TARGET = "Target"

    # ---- Mapplet ports. Rated Low difficulty, and a gap inside a feature
    # already listed as covered: MAPPLET converts but its own ports did not.
    INPUT = "Input"
    OUTPUT = "Output"

    # ---- IDMC Cloud Data Integration transformations with no PowerCenter
    # equivalent. Present so they are recognised and reported by name; a
    # converter exists only where noted in the coverage docs.
    ACCESS_POLICY = "Access Policy"
    B2B = "B2B"
    CHUNKING = "Chunking"
    CLEANSE = "Cleanse"
    DATA_MASKING = "Data Masking"
    DATA_SERVICES = "Data Services"
    DEDUPLICATE = "Deduplicate"
    HIERARCHY_BUILDER = "Hierarchy Builder"
    HIERARCHY_PARSER = "Hierarchy Parser"
    HIERARCHY_PROCESSOR = "Hierarchy Processor"
    LABELER = "Labeler"
    MACHINE_LEARNING = "Machine Learning"
    PARSE = "Parse"
    PYTHON = "Python"
    RULE_SPECIFICATION = "Rule Specification"
    STRUCTURE_PARSER = "Structure Parser"
    VECTOR_EMBEDDING = "Vector Embedding"
    VELOCITY = "Velocity"
    VERIFIER = "Verifier"
    WEB_SERVICES = "Web Services"

    # ---- PowerCenter transformations with no IDMC equivalent.
    EXTERNAL_PROCEDURE = "External Procedure"
    UNSTRUCTURED_DATA = "Unstructured Data"
    WEB_SERVICES_CONSUMER = "Web Services Consumer"
    APPLICATION_SOURCE_QUALIFIER = "Application Source Qualifier"
    MQ_SOURCE_QUALIFIER = "MQ Source Qualifier"
    XML_SOURCE_QUALIFIER = "XML Source Qualifier"

    UNKNOWN = "Unknown"


class LoadStrategy(Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UPSERT = "UPSERT"           # MERGE
    DELETE = "DELETE"
    SCD_TYPE1 = "SCD_TYPE1"     # Overwrite
    SCD_TYPE2 = "SCD_TYPE2"     # History tracking
    TRUNCATE_INSERT = "TRUNCATE_INSERT"


class DataFlowDirection(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    INPUT_OUTPUT = "INPUT_OUTPUT"
    # A PowerCenter/IICS "variable" port: evaluated row-by-row in declaration
    # order, may be referenced by later ports (including outputs), and is
    # not itself part of the target schema. Ported from the Rust reference
    # (the Rust reference implementation, ast.rs PortDirection::Variable) -- see xml_parser's
    # _resolve_direction and transformation_converter's _expression.
    VARIABLE = "VARIABLE"


@dataclass
class FieldMapping:
    source_field: str
    target_field: str
    expression: str = ""
    datatype: str = "STRING"
    precision: int = 0
    scale: int = 0
    nullable: bool = True
    is_key: bool = False
    description: str = ""
    # A flat-file field's position: PHYSICALOFFSET / PHYSICALLENGTH (fixed
    # width), FIELDNUMBER order otherwise.
    physical_offset: int = 0
    physical_length: int = 0


@dataclass
class ConnectionInfo:
    name: str
    db_type: str = ""          # ORACLE, SQL_SERVER, ODBC, FLAT_FILE, etc.
    connection_string: str = ""
    schema: str = ""
    username: str = ""
    database: str = ""
    oltp_source: str = ""      # EBS, PSFT, SIEBEL, JDE, etc.


@dataclass
class SourceDefinition:
    name: str
    owner: str = ""
    db_name: str = ""
    table_name: str = ""
    fields: list = field(default_factory=list)
    connection: Optional[ConnectionInfo] = None
    sql_query: str = ""
    extract_type: str = ""     # FULL, INCREMENTAL
    # The <FLATFILE> attributes of a DATABASETYPE="Flat File" source
    # (DELIMITED, DELIMITERS, SKIPROWS, QUOTE_CHARACTER, ...); empty for a
    # relational source. A flat file is not a catalog table: reading it as
    # spark.table(<definition name>) read something else, or nothing.
    flat_file: dict = field(default_factory=dict)


@dataclass
class TargetDefinition:
    name: str
    owner: str = ""
    db_name: str = ""
    table_name: str = ""
    fields: list = field(default_factory=list)
    connection: Optional[ConnectionInfo] = None
    load_strategy: LoadStrategy = LoadStrategy.INSERT
    warehouse_table: str = ""
    scd_type: int = 0
    flat_file: dict = field(default_factory=dict)  # see SourceDefinition.flat_file


@dataclass
class TransformationField:
    name: str
    datatype: str = "STRING"
    # Lookup transformation port flags from PORTTYPE: a LOOKUP port is a
    # lookup-source column (LOOKUP/OUTPUT returns it, plain LOOKUP only
    # takes part in the condition); RETURN marks an unconnected Lookup's
    # return port. A plain "LOOKUP" port used to parse as INPUT.
    is_lookup: bool = False
    is_return: bool = False
    precision: int = 0
    scale: int = 0
    expression: str = ""
    direction: DataFlowDirection = DataFlowDirection.INPUT_OUTPUT
    default_value: str = ""
    description: str = ""
    # Per-port MASTER marker for a Joiner transformation. In PowerCenter this
    # is the MASTER/ISMASTER TRANSFORMFIELD attribute; in IICS the per-field
    # master/isMaster flag (or a portGroup/group of "master"). A Joiner's
    # master-vs-detail designation is a PORT-level property of the transform
    # -- NOT the order in which the upstream sources' connectors happen to
    # appear in the export. Inferring the outer side from connector order
    # silently inverts a Master/Detail Outer Join whenever the detail source
    # is wired first (mirrors the Rust reference implementation, src/ast.rs Port::is_master).
    is_master: bool = False
    # Per-port GROUP BY marker for an Aggregator transformation. In
    # PowerCenter this is the GROUPBY/ISGROUPBY TRANSFORMFIELD attribute (or
    # a PORTTYPE that carries "GROUP BY", e.g. "INPUT/OUTPUT GROUP BY"); in
    # IICS the per-field groupBy/isGroupBy flag. An Aggregator's GROUP BY is
    # defined by these flags when present -- NOT by port direction and NOT
    # by the absence of an expression (mirrors the Rust reference implementation, src/ast.rs
    # Port::is_group_by). See properties.aggregator_group_by_fallback for
    # the heuristic used only when no port carries this flag.
    is_group_by: bool = False
    # Router (and Union) ports belong to a GROUP: the input group's ports
    # carry GROUP="INPUT"; each output group's ports carry that group's
    # name and a REF_FIELD naming the input port they copy. The connectors
    # leaving a Router name the OUTPUT port (TXN_ID1), so this is the only
    # way to know which group -- and therefore which filtered DataFrame --
    # feeds a downstream instance.
    group: str = ""
    ref_field: str = ""


@dataclass
class Transformation:
    name: str
    type: TransformationType = TransformationType.UNKNOWN
    # The platform's own type string, verbatim, as it appeared in the export.
    # Retained even when ``type`` resolves cleanly, because an unrecognised
    # transformation reported as "Unknown" tells a migrator nothing about
    # what was lost -- and because the IDMC type vocabulary is not yet known
    # from a real export, so the raw strings are the evidence that will
    # populate it.
    raw_type: str = ""
    fields: list = field(default_factory=list)
    properties: dict = field(default_factory=dict)
    # For lookups
    lookup_table: str = ""
    lookup_condition: str = ""
    lookup_sql: str = ""
    # "Lookup policy on multiple match" as the export spells it (Use First
    # Value / Use Last Value / Use Any Value / Report Error). "" = absent.
    lookup_policy: str = ""
    # "Dynamic Lookup Cache" = YES. A dynamic cache is a different algorithm
    # (the cache is updated as the session inserts) and is NOT a static
    # broadcast join; the converter must refuse or flag it, never guess.
    lookup_dynamic: bool = False
    lookup_source_filter: str = ""
    # For source qualifiers. "Source Filter" is a WHERE fragment applied at
    # the source database; "User Defined Join" joins the qualifier's several
    # sources; "Select Distinct" de-duplicates the read. All three change
    # which rows the pipeline sees, so dropping them is a silent data defect.
    source_filter: str = ""
    user_defined_join: str = ""
    select_distinct: bool = False
    # For joins
    join_condition: str = ""
    join_type: str = "INNER"
    # For filters
    filter_condition: str = ""
    # For aggregators
    group_by_fields: list = field(default_factory=list)
    # For router
    router_groups: list = field(default_factory=list)
    # For update strategy
    update_strategy_expression: str = ""
    # For sequence generator
    start_value: int = 1
    increment_by: int = 1
    # For sorter
    sort_keys: list = field(default_factory=list)
    sort_direction: str = "ASC"
    # Raw SQL override
    sql_override: str = ""
    description: str = ""


@dataclass
class Connector:
    """Links between transformations in a mapping data flow."""
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str


@dataclass
class Mapping:
    name: str
    description: str = ""
    # Organizational unit from the SOURCE, not a fixed taxonomy:
    #   PowerCenter -> <FOLDER NAME="..."> in the export
    #   IDMC        -> the asset's project / folder
    # Used for output foldering and AIDP deploy paths. "" means unfoldered.
    folder: str = ""
    sources: list = field(default_factory=list)
    targets: list = field(default_factory=list)
    transformations: list = field(default_factory=list)
    connectors: list = field(default_factory=list)
    # {target instance: ORDER} from <TARGETLOADORDER>: load order groups run
    # one after another, each committed before the next starts.
    target_load_order: dict = field(default_factory=dict)
    subject_area: str = ""
    etl_task_name: str = ""
    parameters: dict = field(default_factory=dict)
    # {"$$NAME": declared DATATYPE, lower-case} from MAPPINGVARIABLE.
    parameter_types: dict = field(default_factory=dict)
    # Parse-time notes the generator surfaces at the top of the notebook --
    # e.g. "mapplet mplt_X expanded inline", "reusable transformation Y
    # resolved from the folder-level definition". Informational, not a
    # review item: the point is that the reader can see what the parser
    # did to the export, not that something is wrong.
    notes: list = field(default_factory=list)


@dataclass
class Session:
    name: str
    mapping_name: str = ""
    description: str = ""
    source_connections: dict = field(default_factory=dict)
    target_connections: dict = field(default_factory=dict)
    # ``$$``-named mapping parameters/variables the session overrides.
    # Session ATTRIBUTE elements ("Treat source rows as", "Commit Interval",
    # "Parameter Filename", ...) are session PROPERTIES and live in
    # ``properties`` below -- they used to land here and were emitted into
    # the notebook's parameters cell as Python variables, where a name with
    # a space in it is a SyntaxError.
    parameters: dict = field(default_factory=dict)
    properties: dict = field(default_factory=dict)
    # {target instance name: {attribute name: value}} from
    # SESSTRANSFORMATIONINST -- "Insert", "Update as Update", "Update else
    # Insert", "Delete", "Truncate target table option". Together with
    # properties["Treat source rows as"] this is where PowerCenter records
    # HOW a target is loaded; the TARGET's CONSTRAINT attribute (which the
    # parser used to read for this) is DDL text, not a load strategy.
    target_load_options: dict = field(default_factory=dict)
    # {source instance or its qualifier: {"Source file directory": ...,
    # "Source filename": ..., "Source filetype": ...}} from a File Reader.
    source_file_options: dict = field(default_factory=dict)
    pre_sql: str = ""
    post_sql: str = ""
    commit_interval: int = 10000
    error_handling: str = "STOP"

    @property
    def treat_source_rows_as(self) -> str:
        """Session-level "Treat source rows as": Insert / Update / Delete /
        Data driven (case as exported). "" when the export does not say."""
        for key, value in self.properties.items():
            if key.strip().lower() == "treat source rows as":
                return str(value)
        return ""

    def load_strategy_for(self, target_instance: str) -> "Optional[LoadStrategy]":
        """The load strategy PowerCenter would apply to ``target_instance``
        in this session, or ``None`` when the export carries no session-
        level information for it (the caller then keeps the mapping's own
        default).

        Rules, per the Workflow Manager's target properties:
        - "Treat source rows as" = Data driven -> the Update Strategy
          transformation decides per row; reported as UPSERT so the
          target write cell routes through the DD_STRATEGY partitions.
        - Truncate target table option = YES -> TRUNCATE_INSERT.
        - Treat as Insert (or Insert=YES with no update/delete flag) -> INSERT.
        - Treat as Update: "Update else Insert" -> UPSERT; "Update as
          Update" -> UPDATE; "Update as Insert" -> INSERT.
        - Treat as Delete -> DELETE.
        """
        opts = {
            k.strip().lower(): str(v).strip().upper()
            for k, v in (self.target_load_options.get(target_instance) or {}).items()
        }
        treat = self.treat_source_rows_as.strip().lower()
        if not opts and not treat:
            return None
        yes = lambda key: opts.get(key, "") in ("YES", "TRUE", "1")  # noqa: E731
        if yes("truncate target table option"):
            return LoadStrategy.TRUNCATE_INSERT
        if treat.startswith("data"):
            return LoadStrategy.UPSERT
        if treat == "delete" or (not treat and yes("delete") and not yes("insert")):
            return LoadStrategy.DELETE
        if treat == "update" or (not treat and (yes("update as update") or yes("update else insert"))):
            if yes("update else insert"):
                return LoadStrategy.UPSERT
            if yes("update as insert"):
                return LoadStrategy.INSERT
            return LoadStrategy.UPDATE
        if treat == "insert" or yes("insert"):
            return LoadStrategy.INSERT
        return None


@dataclass
class Workflow:
    name: str
    description: str = ""
    sessions: list = field(default_factory=list)
    dependencies: list = field(default_factory=list)
    parameters: dict = field(default_factory=dict)
    # Raw <SCHEDULEINFO>/<SCHEDULER> attributes exactly as the export wrote
    # them. Kept verbatim rather than pre-interpreted: the generator decides
    # what it can convert and reports the rest, so nothing is silently lost
    # between here and the job definition.
    scheduler: dict = field(default_factory=dict)
    # EVERY task instance in the workflow, not just the sessions -- one dict
    # per instance with at least {"name", "type"}. A workflow's non-session
    # tasks (Worklet, Command, Email, Decision, Timer, Event-Wait, Control,
    # Assignment) have no notebook to run and so cannot become tasks in the
    # generated job, but dropping them at parse time is what made an
    # incomplete job DAG indistinguishable from a complete one.
    tasks: list = field(default_factory=list)
    # {session instance key: {"task": session TASKNAME, "path": [worklet
    # instance, ..., session instance]}}. ``sessions`` holds instance keys
    # -- what links name -- and this maps each back to the SESSION that
    # carries its notebook and properties. Empty for a workflow built by
    # hand, where a session's key is its name.
    session_instances: dict = field(default_factory=dict)
    folder: str = ""
    # Execution order
    execution_order: list = field(default_factory=list)
    execution_plan: str = ""
    subject_area: str = ""


@dataclass
class LineageNode:
    name: str
    node_type: str = ""  # SOURCE, TRANSFORMATION, TARGET
    fields: list = field(default_factory=list)


@dataclass
class LineageEdge:
    source_node: str
    source_field: str
    target_node: str
    target_field: str
    transformation: str = ""


@dataclass
class DataLineage:
    nodes: list = field(default_factory=list)
    edges: list = field(default_factory=list)


@dataclass
class CompatibilityIssue:
    component: str
    issue_type: str       # UNSUPPORTED, DEPRECATED, PARTIAL, WARNING
    description: str
    suggestion: str = ""
    severity: str = "WARNING"  # WARNING, ERROR, INFO


@dataclass
class MigrationResult:
    """Complete result of parsing and analyzing an Informatica export."""
    version: InfaVersion = InfaVersion.UNKNOWN
    version_detail: str = ""
    repository_name: str = ""
    folder_name: str = ""
    mappings: list = field(default_factory=list)
    sessions: list = field(default_factory=list)
    workflows: list = field(default_factory=list)
    lineage: Optional[DataLineage] = None
    compatibility_issues: list = field(default_factory=list)
