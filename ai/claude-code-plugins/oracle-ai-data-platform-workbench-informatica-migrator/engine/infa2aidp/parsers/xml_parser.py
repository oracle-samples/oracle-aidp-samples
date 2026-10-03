"""Parse Informatica PowerCenter XML exports into migration-ready data models."""

import dataclasses
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from infa2aidp.models import (
    Connector,
    ConnectionInfo,
    DataFlowDirection,
    FieldMapping,
    InfaVersion,
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
from infa2aidp.parsers.security import (
    validate_input_size,
    validate_no_xxe,
    validate_xml_nesting_depth,
)
from infa2aidp.parsers.version_detector import detect_version, require_supported
from infa2aidp.properties import (  # noqa: F401 (get_ci/normalize_key re-exported)
    aggregator_group_by_fallback,
    get_ci,
    is_truthy,
    normalize_key,
)

logger = logging.getLogger(__name__)

from ..transformation_types import resolve as _resolve_type

# Retained for backward compatibility: tests and callers import this.
# Resolution itself now lives in transformation_types.resolve().
_TX_TYPE_LOOKUP = {t.value.lower(): t for t in TransformationType}


class XmlParseError(Exception):
    """A named XML input could not be parsed.

    Raised instead of logging and returning an empty ``MigrationResult`` --
    a result with zero mappings is indistinguishable from a legitimately
    empty export. Batch callers that iterate over many
    files (``migrator.run_migration``, ``cli._cmd_analyze``, and
    ``_parse_folder`` below) already catch exceptions per file and skip-
    and-report; that pattern survives this change unchanged. A single
    named file handed to ``InformaticaXMLParser.parse()`` must fail loudly.
    """


def _attr(elem: ET.Element, name: str, default: str = "") -> str:
    """Case-insensitive XML attribute lookup.

    PowerCenter exports are typically upper-case (``NAME``, ``PORTTYPE``,
    ``EXPRESSION``), but some exports use mixed or lower case. An exact
    ``elem.attrib.get(name)`` silently returns ``default`` on any casing
    difference, which is the same silent-drop defect class as the
    TABLEATTRIBUTE value lookups below. Mirrors the upstream Rust parser's
    ``attr_ci`` (the Rust reference implementation, powercentre.rs:447-452): try the exact
    name first (fast path), then fall back to a case-insensitive scan.
    """
    if name in elem.attrib:
        return elem.attrib[name]
    target = name.lower()
    for key, value in elem.attrib.items():
        if key.lower() == target:
            return value
    return default


def _int_attr(elem: ET.Element, name: str, default: int = 0) -> int:
    val = _attr(elem, name, "")
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _task_properties(tk_elem: ET.Element) -> dict:
    """A <TASK>'s settings: its ATTRIBUTEs, plus its VALUEPAIRs.

    A real PowerCenter Command task keeps its command lines as
    ``<VALUEPAIR NAME="command1" VALUE="mv ..."/>``, not as an ATTRIBUTE,
    so reading ATTRIBUTEs alone lost exactly the setting the review exists
    to quote. Each VALUEPAIR is kept under its own name, and a Command
    task's lines are also joined, in EXECORDER, under ``Command`` -- the
    name the rebuild guidance quotes -- unless an ATTRIBUTE already set it.
    """
    props: dict = {}
    for a in tk_elem.iter("ATTRIBUTE"):
        a_name = _attr(a, "NAME")
        a_val = _attr(a, "VALUE")
        if a_name and a_val:
            props[a_name] = a_val
    pairs = [
        (_int_attr(vp, "EXECORDER", i), _attr(vp, "NAME"), _attr(vp, "VALUE"))
        for i, vp in enumerate(tk_elem.iter("VALUEPAIR"))
    ]
    pairs = [p for p in pairs if p[1] and p[2]]
    for _, vp_name, vp_val in pairs:
        props.setdefault(vp_name, vp_val)
    if pairs and _attr(tk_elem, "TYPE").upper() == "COMMAND":
        props.setdefault("Command", "\n".join(v for _, _, v in sorted(pairs, key=lambda p: p[0])))
    return props


# normalize_key/get_ci now live in infa2aidp.properties --
# imported above and re-exported here so this module's own call sites below,
# and anything importing them from this module (e.g. tests), keep working
# unchanged.


def _resolve_tx_type(type_str: str) -> TransformationType:
    """Map an XML TYPE attribute to a TransformationType enum.

    Delegates to ``transformation_types.resolve``, shared with the IICS
    parser so both platforms land on the same enum member.

    This used to fall back to substring matching when no exact match was
    found. That was removed: with the enum expanded to cover both platforms
    it mis-resolves rather than helps -- ``Parse`` is a substring of ``XML
    Parser``, ``Input`` of ``Input Transformation`` -- and it produced
    confident wrong code instead of a reported gap. Unmatched types now
    return UNKNOWN and the caller records ``raw_type``.
    """
    return _resolve_type(type_str)


def _flat_file(elem: ET.Element) -> dict:
    """The <FLATFILE> attributes of a DATABASETYPE="Flat File" source or
    target, or {} for a relational one."""
    if "flat file" not in _attr(elem, "DATABASETYPE").lower():
        return {}
    ff = elem.find("FLATFILE")
    attrs = dict(ff.attrib) if ff is not None else {}
    attrs.setdefault("DELIMITED", "YES")
    return attrs


def _resolve_direction(porttype: str) -> DataFlowDirection:
    pt = porttype.upper()
    # "VARIABLE" and "LOCAL VARIABLE" both carry neither INPUT nor OUTPUT in
    # the PORTTYPE text, so this must be checked before the INPUT/OUTPUT
    # substring tests below -- otherwise every v_* port silently falls
    # through to INPUT and transformation_converter.py drops its expression
    # (matches the Rust port's PortDirection::Variable).
    if "VARIABLE" in pt:
        return DataFlowDirection.VARIABLE
    if "INPUT" in pt and "OUTPUT" in pt:
        return DataFlowDirection.INPUT_OUTPUT
    if "OUTPUT" in pt:
        return DataFlowDirection.OUTPUT
    return DataFlowDirection.INPUT


class InformaticaXMLParser:
    """Parses Informatica PowerCenter XML exports into MigrationResult."""

    def parse(self, xml_path: str) -> MigrationResult:
        """Parse a single XML file or a folder of XML files.

        Args:
            xml_path: Path to an XML file or directory containing XML files.

        Returns:
            MigrationResult with all extracted components.
        """
        path = Path(xml_path)
        if path.is_dir():
            return self._parse_folder(path)
        return self._parse_file(path)

    def _parse_folder(self, folder: Path) -> MigrationResult:
        """Merge results from all XML files in a folder (recursive, case-insensitive)."""
        result = MigrationResult()
        xml_files = sorted(
            p for p in folder.rglob("*")
            if p.is_file() and p.suffix.lower() == ".xml"
        )
        if not xml_files:
            logger.warning("No XML files found in %s", folder)
            return result

        for xml_file in xml_files:
            # A directory is a batch: one bad file must not abort every
            # other file in it. This is the same skip-and-report policy
            # already applied at the CLI/migrator layer for multi-file
            # inputs (migrator.run_migration, cli._cmd_analyze) -- _parse_file
            # itself now raises XmlParseError loudly for a single named
            # file, so a folder-level batch must catch it
            # here to keep that same "batch survives, single file doesn't
            # silently vanish" contract.
            try:
                partial = self._parse_file(xml_file)
            except XmlParseError as exc:
                logger.warning("Skipping %s: %s", xml_file, exc)
                continue
            result.mappings.extend(partial.mappings)
            result.sessions.extend(partial.sessions)
            result.workflows.extend(partial.workflows)
            result.compatibility_issues.extend(partial.compatibility_issues)
            # Keep first valid version / repo info
            if result.version_detail == "" and partial.version_detail:
                result.version = partial.version
                result.version_detail = partial.version_detail
            if not result.repository_name and partial.repository_name:
                result.repository_name = partial.repository_name
            if not result.folder_name and partial.folder_name:
                result.folder_name = partial.folder_name

        return result

    def _parse_file(self, path: Path) -> MigrationResult:
        """Parse a single Informatica XML export file."""
        result = MigrationResult()
        str_path = str(path)

        # Version detection. Detection of old releases is kept regardless
        # (see version_detector._VERSION_MAP) so the tool can name exactly
        # which release it saw; the gate below only fires once a version is
        # actually known. UNKNOWN is deliberately NOT gated here: it covers
        # both "no REPOSITORY_VERSION attribute at all" (e.g. a
        # transformation-only fragment with no POWERMART wrapper -- several
        # committed test fixtures are exactly this) and malformed/missing
        # files, neither of which is "we identified an out-of-scope
        # PowerCenter release." Only a confidently detected pre-10.x
        # version (V8/V9) is refused here.
        result.version, result.version_detail = detect_version(str_path)
        if result.version is not InfaVersion.UNKNOWN:
            require_supported(result.version, result.version_detail)

        try:
            with open(str_path, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read()
        except OSError as exc:
            raise XmlParseError(f"Could not read {path}: {exc}") from exc

        # Security guards -- run BEFORE any XML parsing, on the raw text, so
        # a customer-supplied export is vetted for oversized input, XXE, and
        # pathological nesting depth before ET ever sees it.
        # detect_version() above already applies the same three guards
        # independently on its own read of this file; running them again
        # here is deliberate defense-in-depth (mirrors security.py's own
        # two-layer XXE rationale), not redundancy to be "cleaned up".
        validate_input_size(content)
        validate_no_xxe(content)
        validate_xml_nesting_depth(content)

        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            # A parse failure must raise, not silently return a
            # zero-mapping result indistinguishable from a legitimately
            # empty export. Per-file skip-and-report for a
            # BATCH of files is still fine -- see _parse_folder above and
            # migrator.run_migration / cli._cmd_analyze, which already wrap
            # this call per file -- but a single named file must fail loudly.
            raise XmlParseError(f"Malformed XML in {path}: {exc}") from exc

        # Repository and folder metadata
        repo = root.find(".//REPOSITORY")
        if repo is not None:
            result.repository_name = _attr(repo, "NAME")

        folder = root.find(".//FOLDER")
        if folder is not None:
            result.folder_name = _attr(folder, "NAME")

        # Collect all sources and targets at folder level (reusable definitions)
        source_defs = {}
        target_defs = {}
        for src in root.iter("SOURCE"):
            sd = self._parse_source(src)
            source_defs[sd.name] = sd
        for tgt in root.iter("TARGET"):
            td = self._parse_target(tgt)
            target_defs[td.name] = td

        # Reusable transformations and mapplets are FOLDER-level objects in a
        # PowerCenter export: a mapping that uses one carries only an
        # <INSTANCE TRANSFORMATION_NAME="..." REUSABLE="YES"> pointing at it,
        # never the <TRANSFORMATION> itself. Reading only the mapping's own
        # children therefore dropped every reusable Expression/Lookup/
        # Sequence Generator and every mapplet from the mapping -- the
        # notebook still generated, with that logic silently absent. Index
        # both here (keyed by name, parent must be the FOLDER, not a MAPPING
        # or MAPPLET) so _parse_mapping can resolve instances against them.
        parent_of = {child: parent for parent in root.iter() for child in parent}
        reusable_defs: dict[str, ET.Element] = {}
        for tx_elem in root.iter("TRANSFORMATION"):
            parent = parent_of.get(tx_elem)
            if parent is not None and parent.tag.upper() in ("FOLDER", "REPOSITORY", "POWERMART"):
                reusable_defs[_attr(tx_elem, "NAME")] = tx_elem
        mapplet_defs: dict[str, ET.Element] = {
            _attr(m, "NAME"): m for m in root.iter("MAPPLET")
        }

        # Parse mappings
        for mapping_elem in root.iter("MAPPING"):
            mapping = self._parse_mapping(
                mapping_elem, source_defs, target_defs, folder_name=result.folder_name,
                reusable_defs=reusable_defs, mapplet_defs=mapplet_defs,
            )
            result.mappings.append(mapping)

        # Parse sessions
        for session_elem in root.iter("SESSION"):
            session = self._parse_session(session_elem)
            result.sessions.append(session)

        # Parse workflows. Worklets are indexed first: a reusable worklet is
        # a folder-level <WORKLET> that a workflow references by name from a
        # TASKINSTANCE, exactly like a reusable transformation, and a
        # non-reusable one is nested inside the WORKFLOW that owns it.
        worklet_defs: dict[str, ET.Element] = {}
        for wl_elem in root.iter("WORKLET"):
            worklet_defs.setdefault(_attr(wl_elem, "NAME"), wl_elem)

        # <TASK> definitions carry the properties a non-session task needs to
        # be rebuilt by hand: the Command task's command line, the Timer's
        # delay, the Email's recipient, the Decision's expression. A
        # TASKINSTANCE only names the task, so without this the review could
        # say a Command was dropped but not WHAT it ran -- which makes the
        # report true and useless.
        #
        # Only folder-level (reusable) <TASK>s are indexed here. A
        # non-reusable one lives inside the <WORKFLOW>/<WORKLET> that runs it
        # and its name is unique only there; indexing every <TASK> in the
        # file by name gave each workflow the FIRST workflow's command.
        # _flatten_task_graph resolves the container's own <TASK>s first.
        task_defs: dict[str, dict] = {}
        for tk_elem in root.iter("TASK"):
            name = _attr(tk_elem, "NAME")
            parent = parent_of.get(tk_elem)
            if not name or parent is None or parent.tag.upper() not in (
                "FOLDER", "REPOSITORY", "POWERMART",
            ):
                continue
            task_defs.setdefault(name, _task_properties(tk_elem))

        for wf_elem in root.iter("WORKFLOW"):
            workflow = self._parse_workflow(wf_elem, worklet_defs, task_defs)
            workflow.folder = result.folder_name or ""
            result.workflows.append(workflow)

        return result

    # ------------------------------------------------------------------
    # Sources
    # ------------------------------------------------------------------

    def _parse_source(self, elem: ET.Element) -> SourceDefinition:
        sd = SourceDefinition(
            name=_attr(elem, "NAME"),
            db_name=_attr(elem, "DBDNAME"),
            owner=_attr(elem, "OWNERNAME"),
        )
        sd.table_name = sd.name
        for sf in elem.findall("SOURCEFIELD"):
            sd.fields.append(FieldMapping(
                source_field=_attr(sf, "NAME"),
                target_field=_attr(sf, "NAME"),
                datatype=_attr(sf, "DATATYPE", "STRING"),
                precision=_int_attr(sf, "PRECISION"),
                scale=_int_attr(sf, "SCALE"),
                nullable=_attr(sf, "NULLABLE", "NULL") != "NOTNULL",
                is_key="PRIMARY" in _attr(sf, "KEYTYPE", "NOT A KEY").upper(),
                physical_offset=_int_attr(sf, "PHYSICALOFFSET"),
                physical_length=_int_attr(sf, "PHYSICALLENGTH"),
            ))
        sd.flat_file = _flat_file(elem)
        return sd

    # ------------------------------------------------------------------
    # Targets
    # ------------------------------------------------------------------

    def _parse_target(self, elem: ET.Element) -> TargetDefinition:
        td = TargetDefinition(
            name=_attr(elem, "NAME"),
            db_name=_attr(elem, "DBDNAME", ""),
            owner=_attr(elem, "OWNERNAME", ""),
        )
        td.table_name = td.name
        td.flat_file = _flat_file(elem)
        # TARGET/@CONSTRAINT is table-constraint DDL text ("PRIMARY KEY
        # (ID)", "FOREIGN KEY ... ON UPDATE CASCADE"), not a load strategy;
        # the previous keyword scan over it turned an "ON UPDATE" clause into
        # LoadStrategy.UPDATE. How a target is loaded is recorded on the
        # SESSION (Treat source rows as + the target instance's Insert/
        # Update/Delete/Truncate flags) -- see Session.load_strategy_for,
        # which the notebook generator consults. The definition-level
        # default stays INSERT.
        for tf in elem.findall("TARGETFIELD"):
            td.fields.append(FieldMapping(
                source_field=_attr(tf, "NAME"),
                target_field=_attr(tf, "NAME"),
                datatype=_attr(tf, "DATATYPE", "STRING"),
                precision=_int_attr(tf, "PRECISION"),
                scale=_int_attr(tf, "SCALE"),
                nullable=_attr(tf, "NULLABLE", "NULL") != "NOTNULL",
                is_key="PRIMARY" in _attr(tf, "KEYTYPE", "NOT A KEY").upper(),
            ))
        return td

    # ------------------------------------------------------------------
    # Mappings
    # ------------------------------------------------------------------

    def _parse_mapping(
        self,
        elem: ET.Element,
        source_defs: dict,
        target_defs: dict,
        folder_name: str = "",
        reusable_defs: Optional[dict] = None,
        mapplet_defs: Optional[dict] = None,
    ) -> Mapping:
        reusable_defs = reusable_defs or {}
        mapplet_defs = mapplet_defs or {}
        name = _attr(elem, "NAME")

        # Parse parameters (MAPPINGVARIABLE)
        params: dict[str, str] = {}
        param_types: dict[str, str] = {}
        for mv in elem.findall("MAPPINGVARIABLE"):
            pname = _attr(mv, "NAME")
            pval = _attr(mv, "DEFAULTVALUE")
            if pname:
                params[pname] = pval
                # The declared datatype decides what the value IS: a
                # date/time parameter's value is a date, not its text.
                param_types[pname] = _attr(mv, "DATATYPE").lower()

        mapping = Mapping(
            name=name,
            description=_attr(elem, "DESCRIPTION"),
            folder=folder_name,
            parameters=params,
        )
        mapping.parameter_types = param_types

        # Instances tell us which sources/targets are used in this mapping
        for inst in elem.findall("INSTANCE"):
            inst_type = _attr(inst, "TYPE").upper()
            inst_name = _attr(inst, "NAME")
            tx_name = _attr(inst, "TRANSFORMATION_NAME")
            tx_type = _attr(inst, "TRANSFORMATION_TYPE")

            if inst_type == "SOURCE":
                ref = source_defs.get(tx_name)
                if ref:
                    mapping.sources.append(ref)
            elif inst_type == "TARGET":
                ref = target_defs.get(tx_name)
                if ref:
                    # Connectors name the INSTANCE; one definition loaded by
                    # three instances (an SCD2 mapping's insert, expire and
                    # new-version paths) is three targets writing one table.
                    # Keyed by definition they collapsed into three copies
                    # of the first, all fed the same DataFrame.
                    if inst_name and inst_name != ref.name:
                        ref = dataclasses.replace(
                            ref, name=inst_name, table_name=ref.table_name or ref.name,
                        )
                    mapping.targets.append(ref)

        for tlo in elem.findall("TARGETLOADORDER"):
            try:
                mapping.target_load_order[_attr(tlo, "TARGETINSTANCE")] = int(_attr(tlo, "ORDER") or 1)
            except ValueError:
                pass

        # Transformations local to the mapping
        for tx_elem in elem.findall("TRANSFORMATION"):
            tx = self._parse_transformation(tx_elem)
            mapping.transformations.append(tx)

        # Instances of folder-level reusable transformations and mapplets.
        # An instance may carry a name different from the definition's
        # (the same reusable Expression used twice in one mapping), and the
        # CONNECTORs reference the INSTANCE name -- so the resolved copy is
        # named after the instance.
        self._resolve_instances(elem, mapping, reusable_defs, mapplet_defs)

        # Connectors
        #
        # FROMINSTANCE/TOINSTANCE is the spelling every real PowerCenter
        # export we've inspected uses (tests/fixtures/corpus/orders_transform.xml,
        # tests/fixtures/powercenter/multi_stage_invoice_dw.xml) and it is what our own
        # fixtures used before converted them. FROMTRANSFORMATION/
        # TOTRANSFORMATION is read as a fallback only: it's what the
        # upstream Rust parser reads (the Rust reference implementation, powercentre.rs:435)
        # and what its own "real_powermart" inline fixture uses -- and we
        # have no licensed PowerCenter install to rule out some version or
        # export path emitting it instead. Reading both costs nothing;
        # reading only one and being wrong means a mapping's internal DAG
        # silently comes out fully disconnected (every transformation
        # collapses to in-degree zero and gets ordered alphabetically
        # instead of by dataflow -- see fix-round-1 report for the
        # regression this exact gap caused). Do not narrow this back to one
        # spelling without a licensed export to confirm which one PowerCenter
        # actually emits.
        rewrites = mapping.__dict__.pop("_mapplet_rewrites", {})
        for conn in elem.findall("CONNECTOR"):
            c = Connector(
                from_instance=_attr(conn, "FROMINSTANCE") or _attr(conn, "FROMTRANSFORMATION"),
                from_field=_attr(conn, "FROMFIELD"),
                to_instance=_attr(conn, "TOINSTANCE") or _attr(conn, "TOTRANSFORMATION"),
                to_field=_attr(conn, "TOFIELD"),
            )
            if c.to_instance in rewrites:
                fed_by_in_port, _ = rewrites[c.to_instance]
                for inner_inst, inner_field in fed_by_in_port.get(c.to_field, []):
                    mapping.connectors.append(Connector(
                        from_instance=c.from_instance, from_field=c.from_field,
                        to_instance=inner_inst, to_field=inner_field,
                    ))
                continue
            if c.from_instance in rewrites:
                _, feeds_out_port = rewrites[c.from_instance]
                src = feeds_out_port.get(c.from_field)
                if src is not None:
                    mapping.connectors.append(Connector(
                        from_instance=src[0], from_field=src[1],
                        to_instance=c.to_instance, to_field=c.to_field,
                    ))
                else:
                    mapping.notes.append(
                        f"mapplet {c.from_instance} output port {c.from_field} has no "
                        f"internal producer -- connector to {c.to_instance}.{c.to_field} dropped"
                    )
                continue
            mapping.connectors.append(c)

        return mapping

    def _resolve_instances(
        self,
        elem: ET.Element,
        mapping: Mapping,
        reusable_defs: dict,
        mapplet_defs: dict,
    ) -> None:
        """Materialise every <INSTANCE TYPE="TRANSFORMATION"> of ``elem``
        that has no local <TRANSFORMATION> definition.

        - A reusable transformation is parsed from its folder-level
          definition and renamed to the instance name.
        - A mapplet is expanded inline (see ``_expand_mapplet``).
        - Anything else is recorded as an UNKNOWN transformation carrying
          the instance's type string, so the missing definition is visible
          in the notebook as an "Unsupported transformation type" marker
          instead of a silently disconnected DAG.
        """
        local = {tx.name for tx in mapping.transformations}
        for inst in elem.findall("INSTANCE"):
            # A mapping export writes a mapplet instance as TYPE="MAPPLET"
            # (TRANSFORMATION_TYPE="Mapplet"); reading only TRANSFORMATION
            # dropped every mapplet, silently.
            if _attr(inst, "TYPE").upper() not in ("TRANSFORMATION", "MAPPLET"):
                continue
            inst_name = _attr(inst, "NAME")
            def_name = _attr(inst, "TRANSFORMATION_NAME") or inst_name
            inst_type = _attr(inst, "TRANSFORMATION_TYPE")
            if inst_name in local:
                continue
            if inst_type.strip().lower() == "mapplet" or (
                def_name in mapplet_defs and def_name not in reusable_defs
            ):
                mapplet_elem = mapplet_defs.get(def_name)
                if mapplet_elem is None:
                    mapping.transformations.append(
                        Transformation(name=inst_name, type=TransformationType.MAPPLET,
                                       raw_type=inst_type or "Mapplet")
                    )
                    mapping.notes.append(
                        f"mapplet instance {inst_name} references mapplet {def_name}, "
                        f"which is not in this export -- its logic is NOT in this notebook"
                    )
                else:
                    self._expand_mapplet(mapping, inst_name, mapplet_elem, reusable_defs)
                local.add(inst_name)
                continue
            def_elem = reusable_defs.get(def_name)
            if def_elem is not None:
                tx = self._parse_transformation(def_elem)
                tx.name = inst_name
                tx.properties["reusable"] = True
                tx.properties["definition_name"] = def_name
                mapping.transformations.append(tx)
                mapping.notes.append(
                    f"reusable transformation {inst_name} resolved from the folder-level "
                    f"definition {def_name}"
                )
            else:
                mapping.transformations.append(
                    Transformation(name=inst_name, type=TransformationType.UNKNOWN,
                                   raw_type=inst_type)
                )
                mapping.notes.append(
                    f"instance {inst_name} ({inst_type}) references transformation "
                    f"{def_name}, which is not defined in this export"
                )
            local.add(inst_name)

    def _expand_mapplet(
        self,
        mapping: Mapping,
        inst_name: str,
        mapplet_elem: ET.Element,
        reusable_defs: dict,
    ) -> None:
        """Inline a mapplet instance into ``mapping``.

        A mapplet is a folder-level object holding its own TRANSFORMATIONs,
        INSTANCEs and CONNECTORs, with an Input Transformation and an Output
        Transformation as its boundary. The mapping's own CONNECTORs address
        the mapplet instance by name with the boundary port as the field.
        Expansion prefixes every internal instance with ``<instance>__``
        (so two instances of the same mapplet do not collide), copies the
        internal transformations into the mapping, and re-wires:

        - mapping -> mapplet.<in_port>   becomes   mapping -> <internal tx fed by in_port>
        - mapplet.<out_port> -> mapping  becomes   <internal tx feeding out_port> -> mapping

        The Input/Output transformations themselves are not copied; they
        carry no logic (see transformation_converter._mapplet_port).
        """
        prefix = f"{inst_name}__"
        inner = Mapping(name=inst_name)
        for tx_elem in mapplet_elem.findall("TRANSFORMATION"):
            inner.transformations.append(self._parse_transformation(tx_elem))
        self._resolve_instances(mapplet_elem, inner, reusable_defs, {})

        boundary_in = {
            tx.name for tx in inner.transformations
            if tx.type is TransformationType.INPUT
            or (tx.raw_type or "").strip().lower() in ("input transformation", "input")
        }
        boundary_out = {
            tx.name for tx in inner.transformations
            if tx.type is TransformationType.OUTPUT
            or (tx.raw_type or "").strip().lower() in ("output transformation", "output")
        }

        internal_conns = [
            Connector(
                from_instance=_attr(c, "FROMINSTANCE") or _attr(c, "FROMTRANSFORMATION"),
                from_field=_attr(c, "FROMFIELD"),
                to_instance=_attr(c, "TOINSTANCE") or _attr(c, "TOTRANSFORMATION"),
                to_field=_attr(c, "TOFIELD"),
            )
            for c in mapplet_elem.findall("CONNECTOR")
        ]
        # in_port -> [(internal instance, field)], out_port -> (internal instance, field)
        fed_by_in_port: dict[str, list[tuple[str, str]]] = {}
        feeds_out_port: dict[str, tuple[str, str]] = {}
        for c in internal_conns:
            if c.from_instance in boundary_in:
                fed_by_in_port.setdefault(c.from_field, []).append((prefix + c.to_instance, c.to_field))
            elif c.to_instance in boundary_out:
                feeds_out_port[c.to_field] = (prefix + c.from_instance, c.from_field)
            else:
                mapping.connectors.append(Connector(
                    from_instance=prefix + c.from_instance, from_field=c.from_field,
                    to_instance=prefix + c.to_instance, to_field=c.to_field,
                ))

        for tx in inner.transformations:
            if tx.name in boundary_in or tx.name in boundary_out:
                continue
            tx.name = prefix + tx.name
            tx.properties["mapplet_instance"] = inst_name
            mapping.transformations.append(tx)

        # Re-wire the mapping's connectors that touch the mapplet instance.
        # Done here (before _parse_mapping reads the mapping-level CONNECTORs)
        # by stashing the rewrite rules on the mapping; _parse_mapping applies
        # them as it reads each connector.
        rules = mapping.__dict__.setdefault("_mapplet_rewrites", {})
        rules[inst_name] = (fed_by_in_port, feeds_out_port)
        unwired_in = sorted(set(fed_by_in_port) - {f for f in fed_by_in_port if fed_by_in_port[f]})
        mapping.notes.append(
            f"mapplet {inst_name} ({_attr(mapplet_elem, 'NAME')}) expanded inline: "
            f"{len(inner.transformations) - len(boundary_in) - len(boundary_out)} "
            f"transformation(s) prefixed {prefix}"
            + (f"; input ports with no internal consumer: {unwired_in}" if unwired_in else "")
        )

    # ------------------------------------------------------------------
    # Transformations
    # ------------------------------------------------------------------

    def _parse_transformation(self, elem: ET.Element) -> Transformation:
        name = _attr(elem, "NAME")
        raw_type = _attr(elem, "TYPE")
        tx_type = _resolve_tx_type(raw_type)
        # PowerCenter implements Union as a Custom Transformation: the export
        # says TYPE="Custom Transformation" TEMPLATENAME="Union
        # Transformation". Read by TYPE alone it resolved to CUSTOM and the
        # notebook passed one input through and dropped the others.
        template = _attr(elem, "TEMPLATENAME")
        if template and "union" in template.lower():
            tx_type = TransformationType.UNION
            raw_type = template
        reusable = _attr(elem, "REUSABLE", "NO").upper() == "YES"

        tx = Transformation(name=name, type=tx_type, raw_type=raw_type)
        tx.properties["reusable"] = reusable

        # Fields
        for tf_elem in elem.findall("TRANSFORMFIELD"):
            # A Joiner's master-vs-detail split is a per-port marker, not a
            # transformation-level property -- PowerCenter carries it as
            # either MASTER or ISMASTER on the TRANSFORMFIELD (exports vary),
            # so both are checked (matches
            # the Rust reference implementation, src/ast.rs Port::is_master).
            porttype_raw = _attr(tf_elem, "PORTTYPE", "INPUT/OUTPUT")
            # Real PowerCenter exports spell the master side into PORTTYPE
            # itself ("INPUT/OUTPUT/MASTER", "INPUT/MASTER"); a Joiner with
            # only that spelling used to have no master at all, so the
            # generator skipped the join and every downstream cell that
            # referenced a master-side column failed (seen on AIDP,
            # 2026-09-24).
            is_master = (
                _attr(tf_elem, "MASTER", "").strip().upper() in ("YES", "TRUE", "1")
                or _attr(tf_elem, "ISMASTER", "").strip().upper() in ("YES", "TRUE", "1")
                or "MASTER" in porttype_raw.upper()
            )
            # PowerCenter marks an Aggregator's grouping ports with a
            # per-port GROUPBY/ISGROUPBY attribute (spelling-tolerant lookup
            # via get_ci, since real exports vary casing/underscores just
            # like every other TABLEATTRIBUTE name in this parser) -- or,
            # in fixtures we have seen in the wild, by suffixing PORTTYPE
            # itself with "GROUP BY" (e.g. "INPUT/OUTPUT GROUP BY"). Either
            # spelling is an explicit per-port flag and is honoured
            # identically by the Aggregator group-by derivation below
            # (matches the Rust reference implementation, src/ast.rs Port::is_group_by
            # / src/parser/powercentre.rs::parse_transformfield).
            # A real Aggregator/Rank export marks a group-by port with
            # EXPRESSIONTYPE="GROUPBY"; with only that spelling the group-by
            # was lost and the aggregation ran over the whole input.
            is_group_by = is_truthy(
                get_ci(dict(tf_elem.attrib), "groupby", "isgroupby")
            ) or "GROUP BY" in porttype_raw.upper() or (
                _attr(tf_elem, "EXPRESSIONTYPE").strip().upper() == "GROUPBY"
            )
            tf = TransformationField(
                name=_attr(tf_elem, "NAME"),
                datatype=_attr(tf_elem, "DATATYPE", "STRING"),
                precision=_int_attr(tf_elem, "PRECISION"),
                scale=_int_attr(tf_elem, "SCALE"),
                expression=_attr(tf_elem, "EXPRESSION"),
                direction=_resolve_direction(porttype_raw),
                default_value=_attr(tf_elem, "DEFAULTVALUE"),
                description=_attr(tf_elem, "DESCRIPTION"),
                is_master=is_master,
                is_group_by=is_group_by,
                group=_attr(tf_elem, "GROUP"),
                ref_field=_attr(tf_elem, "REF_FIELD"),
            )
            tf.is_lookup = "LOOKUP" in porttype_raw.upper()
            tf.is_return = "RETURN" in porttype_raw.upper()
            tx.fields.append(tf)

        # Collect TABLEATTRIBUTE values into a dict for easy access
        table_attrs: dict[str, str] = {}
        for ta in elem.findall("TABLEATTRIBUTE"):
            ta_name = _attr(ta, "NAME")
            ta_val = _attr(ta, "VALUE")
            if ta_name:
                table_attrs[ta_name] = ta_val
                tx.properties[ta_name] = ta_val

        # Type-specific extraction
        self._extract_type_specific(tx, elem, table_attrs)

        return tx

    def _extract_type_specific(
        self,
        tx: Transformation,
        elem: ET.Element,
        table_attrs: dict[str, str],
    ) -> None:
        t = tx.type

        if t == TransformationType.SOURCE_QUALIFIER:
            # "sqlovrd" is the alternate spelling the upstream Rust parser
            # accepts (powercentre.rs:219) alongside "sql query"/"sql_query"
            # (both normalize to "sql query" here).
            tx.sql_override = get_ci(table_attrs, "sql query", "sqlovrd")
            # The three qualifier properties that change WHICH rows are read.
            # A Source Filter is the standard way an incremental PowerCenter
            # load limits its extract; dropping it read the whole table.
            tx.source_filter = get_ci(table_attrs, "source filter")
            tx.user_defined_join = get_ci(table_attrs, "user defined join")
            tx.select_distinct = is_truthy(get_ci(table_attrs, "select distinct", default=""))

        elif t == TransformationType.EXPRESSION:
            # Expressions live on individual fields (already parsed)
            pass

        elif t == TransformationType.LOOKUP:
            # Bare "condition" is the spelling our own shipped fixtures use
            # for a Lookup's join condition (tests/fixtures/corpus/scd_type2.xml,
            # star_schema_fact.xml) -- before this fix, only "Lookup condition"
            # was accepted, so lookup_condition came back "" for those exports
            # with no error, silently dropping the join predicate.
            tx.lookup_condition = get_ci(table_attrs, "lookup condition", "condition")
            tx.lookup_sql = get_ci(table_attrs, "lookup sql override")
            tx.lookup_table = get_ci(table_attrs, "lookup table name")
            # Which lookup row wins on a multiple match, and whether the
            # cache is dynamic -- both were ignored, so a "Report Error"
            # lookup silently became "Use First Value" and a dynamic-cache
            # lookup (the standard SCD insert-or-update pattern) became a
            # static join.
            tx.lookup_policy = get_ci(table_attrs, "lookup policy on multiple match")
            tx.lookup_dynamic = is_truthy(get_ci(table_attrs, "dynamic lookup cache", default=""))
            tx.lookup_source_filter = get_ci(table_attrs, "lookup source filter")

        elif t == TransformationType.FILTER:
            tx.filter_condition = get_ci(table_attrs, "filter condition")

        elif t == TransformationType.JOINER:
            # Upstream also accepts bare "condition" for a Joiner
            # (powercentre.rs:221) -- read it here too, scoped to the
            # Joiner branch so it can't be confused with the Lookup's own
            # "condition" spelling above (each Transformation has exactly
            # one type, so there's no cross-talk between the two branches).
            tx.join_condition = get_ci(table_attrs, "join condition", "condition")
            tx.join_type = get_ci(table_attrs, "join type", default="INNER")

        elif t == TransformationType.AGGREGATOR:
            # Priority 1: TABLEATTRIBUTE "Group by" (most reliable -- an
            # explicit, enumerated, mapping-level list of key names).
            gb_attr = get_ci(table_attrs, "group by")
            if gb_attr:
                tx.group_by_fields = [g.strip() for g in gb_attr.split(",") if g.strip()]
            else:
                # Priority 2: an explicit per-port group-by flag (is_group_by,
                # set above from GROUPBY/ISGROUPBY or a "GROUP BY"-suffixed
                # PORTTYPE) -- if ANY port carries it, honour those flagged
                # ports exactly and stop (mirrors the Rust reference implementation,
                # src/ast.rs::aggregator_group_keys).
                flagged = [
                    f.name for f in tx.fields
                    if isinstance(f, TransformationField) and f.is_group_by
                ]
                if flagged:
                    tx.group_by_fields = flagged
                else:
                    # Priority 3 (fallback heuristic, only reached when NO
                    # port is explicitly flagged): non-aggregated Input
                    # ports, excluding pass-throughs and excluding ports
                    # consumed by an aggregate expression. This is the fix
                    # for -- a bare PORTTYPE="INPUT" port on an
                    # Aggregator with no other marker (e.g. order_to_cash.xml
                    # AGG_REVENUE, time_series_rollup.xml AGG_DAILY_STATS)
                    # used to fall through all three of the old priorities
                    # and silently collapse a per-group aggregation into one
                    # global row. See properties.aggregator_group_by_fallback
                    # (ported from the Rust reference implementation, src/ast.rs) for why a
                    # pass-through port must NOT be inferred as a group key.
                    tx.group_by_fields = aggregator_group_by_fallback(tx.fields)

        elif t == TransformationType.UPDATE_STRATEGY:
            tx.update_strategy_expression = get_ci(
                table_attrs, "update strategy expression"
            )

        elif t == TransformationType.ROUTER:
            # Method 1: GROUP child elements (standard Informatica format).
            # PowerCenter's own DTD uses EXPRESSION for the group condition;
            # we have seen CONDITION in the wild (and use it ourselves in
            # tests/fixtures/powercenter/data_quality_route.xml); the upstream Rust
            # parser instead reads CONDITION or FILTER_CONDITION
            # (powercentre.rs:243-245). Neither one of us reads all three,
            # and there is no licensed PowerCenter install available to
            # settle which spelling is canonical -- so read the UNION of
            # all three rather than picking a side. Do not "tidy" this down
            # to one spelling; that would silently reintroduce the dropped-
            # condition bug for whichever export uses the spelling removed.
            # EXPRESSION wins if a GROUP element somehow carries more than
            # one of them.
            # A PowerCenter Router always carries its INPUT group as a
            # <GROUP TYPE="INPUT"> element with no condition. It is not an
            # output group: treating it as one made it the "default" group
            # (rows matching no condition) and gave every Router an extra
            # phantom output.
            input_group_names = {
                f.group for f in tx.fields
                if isinstance(f, TransformationField)
                and f.direction == DataFlowDirection.INPUT and f.group
            }
            for group in elem.findall("GROUP"):
                if _attr(group, "TYPE").strip().upper() == "INPUT" or (
                    _attr(group, "NAME") in input_group_names
                    and not (_attr(group, "EXPRESSION") or _attr(group, "CONDITION"))
                ):
                    continue
                condition = (
                    _attr(group, "EXPRESSION")
                    or _attr(group, "CONDITION")
                    or _attr(group, "FILTER_CONDITION")
                )
                group_info = {
                    "name": _attr(group, "NAME"),
                    "condition": condition,
                    "order": _int_attr(group, "ORDER"),
                }
                tx.router_groups.append(group_info)
            # Method 2: TABLEATTRIBUTE (alternative format — name=group label, value=condition)
            if not tx.router_groups:
                # Known non-condition attributes to skip
                _skip = {"description", "tracing level", "sorted input",
                          "transformation scope", "cache directory"}
                for attr_name, attr_value in table_attrs.items():
                    if attr_name.lower() in _skip:
                        continue
                    # Check if value looks like a condition (has operators or functions)
                    if attr_value and any(kw in attr_value.upper() for kw in
                                          ("=", ">", "<", "ISNULL", "NOT ", "AND ", "OR ",
                                           "IN (", "LIKE ", "BETWEEN ")):
                        safe_name = attr_name.replace(" ", "_").lower()
                        tx.router_groups.append({
                            "name": safe_name,
                            "condition": attr_value,
                        })

        elif t == TransformationType.RANK:
            # A real export carries "Top/Bottom" (Top = highest first) and
            # "Number Of Ranks", flags the rank port in its PORTTYPE (the
            # Designer's R column; exported with the MASTER bit, or RANK)
            # and the group-by ports with EXPRESSIONTYPE="GROUPBY". The
            # "Rank Data Port" / "Group By Port" / "Rank Order" /
            # "Number of Ranks" attributes read before are kept as a
            # fallback for fixtures that use them.
            rank_data_port = get_ci(table_attrs, "rank data port")
            if not rank_data_port:
                for tf_elem in elem.findall("TRANSFORMFIELD"):
                    pt = _attr(tf_elem, "PORTTYPE").upper()
                    if ("RANK" in pt or "MASTER" in pt) and _attr(tf_elem, "NAME").upper() != "RANKINDEX":
                        rank_data_port = _attr(tf_elem, "NAME")
                        break
            top_bottom = get_ci(table_attrs, "top/bottom")
            rank_order = get_ci(table_attrs, "rank order")
            if top_bottom:
                descending = top_bottom.strip().lower().startswith("top")
            else:
                descending = "asc" not in (rank_order or "Descending").lower()
            group_by_port = get_ci(table_attrs, "group by port")
            top_n = get_ci(table_attrs, "number of ranks")

            if rank_data_port:
                tx.sort_keys = [{"field": rank_data_port, "direction": "DESC" if descending else "ASC"}]
            if group_by_port:
                tx.group_by_fields = [g.strip() for g in group_by_port.split(",") if g.strip()]
            else:
                tx.group_by_fields = [
                    f.name for f in tx.fields
                    if isinstance(f, TransformationField) and f.is_group_by
                ]
            if top_n:
                tx.properties["top_bottom"] = top_n

        elif t == TransformationType.SEQUENCE_GENERATOR:
            tx.start_value = int(
                get_ci(table_attrs, "start value", default="0") or "0"
            )
            tx.increment_by = int(
                get_ci(table_attrs, "increment by", default="1") or "1"
            )

        elif t == TransformationType.SORTER:
            # Each TRANSFORMFIELD carries its OWN SORTDIRECTION -- store it
            # per key (as a {"field", "direction"} dict, the same shape
            # iics_parser.py and the RANK branch above already use) rather
            # than in the single shared tx.sort_direction, which used to be
            # overwritten on every iteration so the LAST key's direction
            # silently applied to every key.
            # tx.sort_direction is still set (to the first key's direction)
            # for any older/simpler consumer keyed to that field, but the
            # per-key dicts are authoritative and are what
            # transformation_converter._sorter reads.
            #
            # PowerCenter exports use TWO shapes for the same information,
            # and a given export uses only one of them (bug #16): some mark
            # each TRANSFORMFIELD with ISSORTKEY="YES"/SORTDIRECTION, others
            # instead emit dedicated <SORTKEY NAME="..." DIRECTION="..."/>
            # child elements alongside the TRANSFORMFIELDs (e.g. the
            # time_series_rollup.xml fixture). Reading only the attribute
            # shape left the SORTKEY-element shape resolving to
            # sort_keys=[] -- a silent no-op, same defect class as the
            # Router EXPRESSION/CONDITION bug: read one shape where the
            # export writes another. Read both.
            first_direction = None
            for tf_elem in elem.findall("TRANSFORMFIELD"):
                sort_key = _attr(tf_elem, "ISSORTKEY", "NO")
                if sort_key.upper() == "YES":
                    direction = _attr(tf_elem, "SORTDIRECTION", "ASC")
                    tx.sort_keys.append({
                        "field": _attr(tf_elem, "NAME"),
                        "direction": direction,
                    })
                    if first_direction is None:
                        first_direction = direction
            for sk_elem in elem.findall("SORTKEY"):
                field = _attr(sk_elem, "NAME")
                if not field:
                    continue
                direction = _attr(sk_elem, "DIRECTION", "ASC")
                tx.sort_keys.append({
                    "field": field,
                    "direction": direction,
                })
                if first_direction is None:
                    first_direction = direction
            if first_direction is not None:
                tx.sort_direction = first_direction

        elif t == TransformationType.TRANSACTION_CONTROL:
            # The Designer calls it "Transaction Control Condition".
            tx.properties["tc_expression"] = get_ci(
                table_attrs, "transaction control condition", "transaction control expression"
            )

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------

    def _parse_session(self, elem: ET.Element) -> Session:
        session = Session(
            name=_attr(elem, "NAME"),
            mapping_name=_attr(elem, "MAPPINGNAME"),
            description=_attr(elem, "DESCRIPTION"),
        )

        # Connection references
        for cr in elem.iter("CONNECTIONREFERENCE"):
            conn = ConnectionInfo(
                name=_attr(cr, "CONNECTIONNAME"),
                db_type=_attr(cr, "CONNECTIONTYPE"),
            )
            cnx_type = _attr(cr, "CONNECTIONSUBTYPE", _attr(cr, "CONNECTIONTYPE"))
            variable = _attr(cr, "VARIABLE", "")
            if "SOURCE" in variable.upper() or "READER" in _attr(cr, "COMPONENTNAME", "").upper():
                session.source_connections[_attr(cr, "INSTANCENAME", conn.name)] = conn
            else:
                session.target_connections[_attr(cr, "INSTANCENAME", conn.name)] = conn

        # Session extension attributes
        for se in elem.iter("SESSIONEXTENSION"):
            for attr_elem in se.findall("ATTRIBUTE"):
                aname = _attr(attr_elem, "NAME")
                aval = _attr(attr_elem, "VALUE")
                if aname == "Pre SQL":
                    session.pre_sql = aval
                elif aname == "Post SQL":
                    session.post_sql = aval
                elif aname == "Commit Interval":
                    try:
                        session.commit_interval = int(aval)
                    except (ValueError, TypeError):
                        pass
                elif aname == "Error handling":
                    session.error_handling = aval

            # Connection info from extension
            conn_ref = se.find("CONNECTIONREFERENCE")
            if conn_ref is not None:
                conn = ConnectionInfo(
                    name=_attr(conn_ref, "CONNECTIONNAME"),
                    db_type=_attr(conn_ref, "CONNECTIONTYPE"),
                )
                se_name = _attr(se, "NAME", "")
                if "SOURCE" in se_name.upper() or "READER" in se_name.upper():
                    session.source_connections[se_name] = conn
                else:
                    session.target_connections[se_name] = conn

        # Top-level attributes on session are session PROPERTIES ("Treat
        # source rows as", "Commit Interval", "Parameter Filename", ...).
        # Only a ``$$``-named entry is a parameter override. Filing every
        # ATTRIBUTE under parameters used to put "Treat source rows as" into
        # the notebook's parameters cell as a Python variable name -- a
        # SyntaxError that made every notebook for a real session invalid.
        for attr_elem in elem.findall("ATTRIBUTE"):
            aname = _attr(attr_elem, "NAME")
            aval = _attr(attr_elem, "VALUE")
            if not aname:
                continue
            if aname.startswith("$$"):
                session.parameters[aname] = aval
            else:
                session.properties[aname] = aval

        # Per-target-instance load flags: Insert / Update as Update / Update
        # else Insert / Update as Insert / Delete / Truncate target table
        # option. With "Treat source rows as" above, this is the export's
        # actual record of how each target is loaded. A real export writes
        # them as ATTRIBUTEs of the target's writer extension
        # (<SESSIONEXTENSION TYPE="WRITER" SINSTANCENAME="...">); they are
        # also read from SESSTRANSFORMATIONINST below.
        for se in elem.iter("SESSIONEXTENSION"):
            if _attr(se, "TYPE").upper() == "READER":
                # A File Reader's "Source file directory" / "Source filename"
                # say where a flat-file source is read from; keyed by the
                # source instance (and its qualifier, DSQINSTNAME).
                opts = {_attr(a, "NAME"): _attr(a, "VALUE") for a in se.findall("ATTRIBUTE")}
                if any("file" in k.lower() for k in opts):
                    for key in (_attr(se, "SINSTANCENAME"), _attr(se, "DSQINSTNAME")):
                        if key:
                            session.source_file_options.setdefault(key, {}).update(opts)
                continue
            if _attr(se, "TYPE").upper() != "WRITER":
                continue
            inst_name = _attr(se, "SINSTANCENAME")
            if not inst_name:
                continue
            opts = session.target_load_options.setdefault(inst_name, {})
            for attr_elem in se.findall("ATTRIBUTE"):
                aname = _attr(attr_elem, "NAME")
                if aname:
                    opts[aname] = _attr(attr_elem, "VALUE")
            opts.setdefault("__type__", _attr(se, "TRANSFORMATIONTYPE"))
        for sti in elem.iter("SESSTRANSFORMATIONINST"):
            inst_name = _attr(sti, "SINSTANCENAME") or _attr(sti, "TRANSFORMATIONNAME")
            if not inst_name:
                continue
            opts = session.target_load_options.setdefault(inst_name, {})
            for attr_elem in sti.findall("ATTRIBUTE"):
                aname = _attr(attr_elem, "NAME")
                if aname:
                    opts[aname] = _attr(attr_elem, "VALUE")
            opts.setdefault("__type__", _attr(sti, "TRANSFORMATIONTYPE"))

        return session

    # ------------------------------------------------------------------
    # Workflows
    # ------------------------------------------------------------------

    def _parse_workflow(
        self, elem: ET.Element, worklet_defs: Optional[dict] = None,
        task_defs: Optional[dict] = None,
    ) -> Workflow:
        wf = Workflow(
            name=_attr(elem, "NAME"),
            description=_attr(elem, "DESCRIPTION"),
        )
        wf.parameters["server_name"] = _attr(elem, "SERVERNAME")
        wf.parameters["is_enabled"] = _attr(elem, "ISENABLED", "YES")

        # Scheduler. Stored raw -- see Workflow.scheduler. PowerCenter puts
        # the detail on a <SCHEDULEINFO> child of <SCHEDULER>, but both
        # shapes appear in the wild (a reusable scheduler carries its own
        # attributes), so merge SCHEDULER's attributes and let SCHEDULEINFO's
        # win where they collide -- SCHEDULEINFO is the more specific of the
        # two. An export with no scheduler leaves this empty, which the
        # generator treats as "no schedule", never as a default.
        sched_elem = elem.find("SCHEDULER")
        if sched_elem is not None:
            wf.scheduler.update(dict(sched_elem.attrib))
            info = sched_elem.find("SCHEDULEINFO")
            if info is not None:
                wf.scheduler.update(dict(info.attrib))

        # Task instances. EVERY instance is recorded, not only the sessions:
        # a workflow that runs Command/Email/Decision tasks between its
        # sessions previously lost those instances here and produced a job
        # DAG that looked complete. Worklets are expanded in place (see
        # _flatten_task_graph); one whose definition is not in this export
        # stays a WORKLET instance and is reported by the generator.
        task_infos, links = self._flatten_task_graph(
            elem, "", (), worklet_defs or {}, (_attr(elem, "NAME"),),
            task_defs or {},
        )
        for task_info in task_infos:
            wf.tasks.append(task_info)
            if str(task_info["type"]).upper() == "SESSION":
                key = task_info["instance_name"]
                wf.sessions.append(key)
                wf.session_instances[key] = {
                    "task": task_info["name"], "path": list(task_info["path"]),
                }
        wf.dependencies.extend(links)

        # Build execution order via topological sort
        wf.execution_order = self._topo_sort(links)

        return wf

    def _flatten_task_graph(
        self,
        container: ET.Element,
        prefix: str,
        path: tuple,
        worklet_defs: dict,
        stack: tuple,
        task_defs: Optional[dict] = None,
    ) -> tuple[list[dict], list[dict]]:
        """Task instances and links of a workflow or worklet, with every
        worklet it runs expanded in place.

        Links name task INSTANCES (TASKINSTANCE NAME), never tasks: a
        reusable session s_m_load_dim run twice as s_dim_customer and
        s_dim_product is two nodes. Reading TASKNAME instead merged them and
        orphaned every link that named an instance.

        A worklet's own instances become nodes keyed ``<worklet>__<inst>``,
        so the same reusable worklet run twice, or a session name reused
        across worklets, cannot collide. Each link into the worklet is
        re-pointed at its entry nodes (no incoming link inside it -- in
        practice its Start task) and each link out of it at its exit nodes
        (no outgoing link inside it). The link keeps its CONDITION, and
        ``condition_task`` keeps the instance the condition was written
        against (``$wl_dims.Status`` names the worklet, not a session in it).

        The definition is looked up among the container's own nested
        <WORKLET> children first (a non-reusable worklet), then at folder
        level. One that is absent from the export, or that would recurse
        into itself, stays a single WORKLET node for the generator to
        report.
        """
        local_defs = {_attr(w, "NAME"): w for w in container.findall("WORKLET")}
        # Non-reusable task definitions are scoped like non-reusable
        # worklets: the container's own first, then folder level.
        local_tasks = {
            _attr(t, "NAME"): _task_properties(t) for t in container.findall("TASK")
        }
        tasks: list[dict] = []
        entries: dict[str, list[str]] = {}
        exits: dict[str, list[str]] = {}
        links: list[dict] = []

        for ti in container.findall("TASKINSTANCE"):
            inst = _attr(ti, "NAME", _attr(ti, "TASKINSTANCEPATH"))
            ttype = _attr(ti, "TASKTYPE")
            key = prefix + inst
            if ttype.upper() == "WORKLET":
                wl_name = _attr(ti, "TASKNAME") or inst
                wl_elem = local_defs.get(wl_name, worklet_defs.get(wl_name))
                if wl_elem is not None and wl_name not in stack:
                    sub_tasks, sub_links = self._flatten_task_graph(
                        wl_elem, key + "__", path + (inst,), worklet_defs,
                        stack + (wl_name,), task_defs,
                    )
                    keys = [st["instance_name"] for st in sub_tasks]
                    has_in = {ln["to_task"] for ln in sub_links}
                    has_out = {ln["from_task"] for ln in sub_links}
                    entries[inst] = [k for k in keys if k not in has_in] or keys
                    exits[inst] = [k for k in keys if k not in has_out] or keys
                    tasks.extend(sub_tasks)
                    links.extend(sub_links)
                    continue
            t_name = _attr(ti, "TASKNAME") or inst
            tasks.append({
                "name": t_name,
                "type": ttype,
                "instance_name": key,
                "path": path + (inst,),
                # Empty when the export carries no <TASK> for this instance.
                "properties": dict(
                    local_tasks[t_name] if t_name in local_tasks
                    else (task_defs or {}).get(t_name, {})
                ),
            })

        for wl in container.findall("WORKFLOWLINK"):
            frm, to = _attr(wl, "FROMTASK"), _attr(wl, "TOTASK")
            cond = _attr(wl, "CONDITION")
            for f in exits.get(frm, [prefix + frm]):
                for d in entries.get(to, [prefix + to]):
                    links.append({
                        "from_task": f, "to_task": d, "condition": cond,
                        "condition_task": frm, "to_instance": prefix + to,
                    })
        return tasks, links

    @staticmethod
    def _topo_sort(links: list[dict]) -> list[str]:
        """Simple topological sort from workflow links."""
        from collections import defaultdict, deque

        graph: dict[str, list[str]] = defaultdict(list)
        in_degree: dict[str, int] = defaultdict(int)
        all_tasks: set[str] = set()

        for link in links:
            src = link["from_task"]
            dst = link["to_task"]
            if not src or not dst:
                continue
            graph[src].append(dst)
            in_degree.setdefault(src, 0)
            in_degree[dst] = in_degree.get(dst, 0) + 1
            all_tasks.update([src, dst])

        queue = deque(t for t in all_tasks if in_degree.get(t, 0) == 0)
        order = []
        while queue:
            node = queue.popleft()
            order.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order


def _attr_from_field(field: TransformationField, attr: str, default: str = "") -> str:
    """Get an attribute from a TransformationField or return default."""
    return getattr(field, attr, default)
