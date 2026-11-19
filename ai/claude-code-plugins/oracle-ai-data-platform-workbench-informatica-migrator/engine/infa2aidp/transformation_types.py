"""Transformation-type resolution, shared by both parsers.

One table, two platforms. PowerCenter spells a lookup ``Lookup Procedure``
and IDMC spells it ``lookup``; both have to land on the same enum member or
the converter dispatch sees two different things.

**Why this module exists rather than a dict comprehension over the enum.**
The previous resolver fell back to substring matching -- ``if k in key or
key in k`` -- which was tolerable while the enum held 23 members that shared
few words. It is not tolerable now: ``Parse`` is a substring of ``XML
Parser``, ``Input`` of ``Input Transformation``, ``Union`` of ``Union
Transformation``. Substring matching would resolve a CDI Parse
transformation to XML_PARSER and generate confident, wrong code. Worse, the
match order depends on dict iteration order, so which wrong answer you get
depends on enum declaration order.

Resolution is therefore exact-match only, against the canonical enum value
and an explicit alias table. Anything unmatched returns ``UNKNOWN`` *and*
the caller keeps the raw string -- see ``Transformation.raw_type``. An
unrecognised type must be reported by name, because "Unsupported
transformation type: Unknown" tells a migrator nothing about what it lost.
"""

from __future__ import annotations

from .models import TransformationType

# Platform spellings -> canonical enum member.
#
# PowerCenter values are the ``TYPE`` attribute of a ``<TRANSFORMATION>``
# element. IDMC values are the ``type`` key of a transformation object; the
# lower-case run-together forms are what the current IICS parser emits.
#
# IDMC-native spellings are marked UNVERIFIED: they are derived from the
# Cloud Data Integration transformation list, not from a real export. The
# literal strings IDMC emits are not yet known -- ``resolve()`` preserves
# whatever arrives, so the first real export will show them and this table
# can be corrected from evidence rather than guessed at again.
_ALIASES: dict[str, TransformationType] = {
    # ---- PowerCenter spellings that differ from the canonical value
    "lookup procedure": TransformationType.LOOKUP,
    "union transformation": TransformationType.UNION,
    "sequence": TransformationType.SEQUENCE_GENERATOR,
    "source definition": TransformationType.SOURCE,
    "target definition": TransformationType.TARGET,
    "input transformation": TransformationType.INPUT,
    "output transformation": TransformationType.OUTPUT,
    "application source qualifier": TransformationType.APPLICATION_SOURCE_QUALIFIER,
    "mq source qualifier": TransformationType.MQ_SOURCE_QUALIFIER,
    "xml source qualifier": TransformationType.XML_SOURCE_QUALIFIER,
    # ---- Bare IDMC spellings of types PowerCenter suffixes with
    # "Transformation". The canonical enum values carry the PowerCenter
    # form, so the short CDI names need explicit entries or a CDI Java
    # transformation resolves to UNKNOWN.
    "java": TransformationType.JAVA,
    "sql": TransformationType.SQL,
    "http": TransformationType.HTTP,
    "custom": TransformationType.CUSTOM,
    # ---- IDMC / IICS run-together spellings (the current parser's vocabulary)
    "sourcequalifier": TransformationType.SOURCE_QUALIFIER,
    "sequencegenerator": TransformationType.SEQUENCE_GENERATOR,
    "updatestrategy": TransformationType.UPDATE_STRATEGY,
    "storedprocedure": TransformationType.STORED_PROCEDURE,
    "transactioncontrol": TransformationType.TRANSACTION_CONTROL,
    "xmlparser": TransformationType.XML_PARSER,
    "xmlgenerator": TransformationType.XML_GENERATOR,
    # ---- IDMC-native types. UNVERIFIED spellings; see module docstring.
    "hierarchyprocessor": TransformationType.HIERARCHY_PROCESSOR,
    "hierarchybuilder": TransformationType.HIERARCHY_BUILDER,
    "hierarchyparser": TransformationType.HIERARCHY_PARSER,
    "structureparser": TransformationType.STRUCTURE_PARSER,
    "vectorembedding": TransformationType.VECTOR_EMBEDDING,
    "machinelearning": TransformationType.MACHINE_LEARNING,
    "rulespecification": TransformationType.RULE_SPECIFICATION,
    "datamasking": TransformationType.DATA_MASKING,
    "dataservices": TransformationType.DATA_SERVICES,
    "accesspolicy": TransformationType.ACCESS_POLICY,
    "webservices": TransformationType.WEB_SERVICES,
    "webservicesconsumer": TransformationType.WEB_SERVICES_CONSUMER,
    "externalprocedure": TransformationType.EXTERNAL_PROCEDURE,
    "unstructureddata": TransformationType.UNSTRUCTURED_DATA,
}

# Canonical value -> member, case-insensitive. Built once.
_CANONICAL: dict[str, TransformationType] = {
    t.value.lower(): t for t in TransformationType
}


def resolve(type_str: str) -> TransformationType:
    """Map a platform type string to a ``TransformationType``.

    Exact match only, against the canonical enum value then the alias
    table. Returns ``UNKNOWN`` for anything else -- deliberately, so that
    an unrecognised type is reported rather than guessed at. Callers must
    retain the original string (``Transformation.raw_type``).
    """
    key = (type_str or "").strip().lower()
    if not key:
        return TransformationType.UNKNOWN
    if key in _CANONICAL:
        return _CANONICAL[key]
    if key in _ALIASES:
        return _ALIASES[key]
    # Tolerate spacing/underscore/hyphen variants of the same word sequence
    # ("Hierarchy_Processor", "hierarchy-processor"). This is normalisation,
    # not fuzzy matching: the collapsed form must still match exactly.
    collapsed = key.replace("_", "").replace("-", "").replace(" ", "")
    if collapsed in _ALIASES:
        return _ALIASES[collapsed]
    for canon, member in _CANONICAL.items():
        if canon.replace(" ", "") == collapsed:
            return member
    return TransformationType.UNKNOWN
