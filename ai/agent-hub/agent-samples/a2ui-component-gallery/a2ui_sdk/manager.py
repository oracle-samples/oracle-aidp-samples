# Copyright 2026 Google LLC
# Modifications Copyright (C) 2026, Oracle and/or its affiliates.
#
# This file includes code adapted from the A2UI SDK and has been modified by Oracle.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import collections
import copy
import glob
import inspect
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Optional
from urllib.parse import urljoin, urlparse

A2UI_OPEN_TAG = "<a2ui-json>"
A2UI_CLOSE_TAG = "</a2ui-json>"
A2UI_SCHEMA_BLOCK_START = "---BEGIN A2UI JSON SCHEMA---"
A2UI_SCHEMA_BLOCK_END = "---END A2UI JSON SCHEMA---"

VERSION_0_8 = "0.8"
VERSION_0_9 = "0.9"
CATALOG_ID_KEY = "catalogId"
CATALOG_COMPONENTS_KEY = "components"
CATALOG_STYLES_KEY = "styles"
ENCODING = "utf-8"
BASE_SCHEMA_URL = "https://a2ui.org/"
SUPPORTED_VERSIONS = (VERSION_0_8, VERSION_0_9)
SUPPORTED_VERSION = VERSION_0_8
SUPPORTED_VERSION_KEY = "v0.9"
DEFAULT_CATALOG_NAME = "default"
V09_CATALOG_ID = "/a2ui_specification/2.0.0/agent_hub_a2ui_custom_component_catalog.json"
V08_CATALOG_ID = "/a2ui_specification/1.0.0/agent_hub_a2ui_custom_component_catalog.json"
DEFAULT_CATALOG_ID = V09_CATALOG_ID
BASIC_CATALOG_NAME = "basic"
CUSTOM_CATALOG_NAME = "custom"
INLINE_CATALOG_NAME = "inline"
SUPPORTED_CATALOG_IDS_KEY = "supportedCatalogIds"
INLINE_CATALOGS_KEY = "inlineCatalogs"
DEFAULT_SERVER_SCHEMA_FILE = "server_to_client.json"
DEFAULT_COMMON_TYPES_FILE = "common_types.json"
DEFAULT_BASIC_CATALOG_FILE = "basic_catalog.json"
DEFAULT_CUSTOM_CATALOG_FILE = "custom_catalog.json"
DEFAULT_COMPLETE_CATALOG_FILE = "complete_catalog.json"
DEFAULT_CATALOG_RULES_FILE = "rules.txt"

MAX_GLOBAL_DEPTH = 50
MAX_FUNC_CALL_DEPTH = 5
COMPONENTS = "components"
ID = "id"
ROOT = "root"
PATH = "path"
FUNCTION_CALL = "functionCall"
CALL = "call"
ARGS = "args"
RELAXED_PATH_PATTERN = re.compile(
    r"^(?:(?:\/(?:[^~\/]|~[01])*)*|(?:[^~\/]|~[01])+(?:\/(?:[^~\/]|~[01])*)*)$"
)

DEFAULT_WORKFLOW_RULES_V08 = f"""
The generated response MUST follow these rules:
- The response can contain one or more A2UI JSON blocks.
- Each A2UI JSON block MUST be wrapped in `{A2UI_OPEN_TAG}` and `{A2UI_CLOSE_TAG}` tags.
- Between or around these blocks, you can provide conversational text.
- The JSON part MUST be a single, raw JSON object (usually a list of A2UI messages) and MUST validate against the provided A2UI JSON SCHEMA.
- Top-Down Component Ordering: Within the `components` list of a message:
  - The 'root' component MUST be the FIRST element.
  - Parent components MUST appear before their child components.
""".strip()

DEFAULT_WORKFLOW_RULES_V09 = f"""
The generated response MUST follow these rules:
- The response can contain one or more A2UI JSON blocks.
- Each A2UI JSON block MUST be wrapped in `{A2UI_OPEN_TAG}` and `{A2UI_CLOSE_TAG}` tags.
- Between or around these blocks, you can provide conversational text.
- The JSON part MUST be a single, raw JSON object or JSON array and MUST validate against the provided A2UI JSON SCHEMA.
- For v0.9, each A2UI message object MUST include `version: "v0.9"` and exactly one message key.
- Each `createSurface` operation must create a distinct `surfaceId`; use that ID only for later updates to the same surface.
""".strip()

DEFAULT_WORKFLOW_RULES_BY_VERSION = {
    "0.8": DEFAULT_WORKFLOW_RULES_V08,
    "0.9": DEFAULT_WORKFLOW_RULES_V09,
}
DEFAULT_WORKFLOW_RULES = DEFAULT_WORKFLOW_RULES_BY_VERSION[SUPPORTED_VERSION]


# Minimal prompt-generation strategy interface.
# Copied over from inference_strategy.py
class InferenceStrategy(ABC):

  @abstractmethod
  def generate_system_prompt(
      self,
      role_description: str,
      workflow_description: str = "",
      ui_description: str = "",
      client_ui_capabilities: Optional[dict[str, Any]] = None,
      allowed_components: Optional[list[str]] = None,
      allowed_messages: Optional[list[str]] = None,
      include_schema: bool = False,
      include_rules: bool = False,
      include_examples: bool = False,
      validate_examples: bool = False,
  ) -> str:
    """
    Generates a system prompt for all LLM requests.

    Args:
      role_description: Description of the agent's role.
      workflow_description: Description of the workflow.
      ui_description: Description of the UI.
      client_ui_capabilities: Capabilities reported by the client for targeted schema pruning.
      allowed_components: List of allowed catalog components.
      allowed_messages: List of allowed messages.
      include_schema: Whether to include the schema.
      include_rules: Whether to load and include a sibling rules.txt file.
      include_examples: Whether to include examples.
      validate_examples: Whether to validate examples.

    Returns:
      The system prompt.
    """
    pass


# Abstract provider for loading a catalog schema.
# Copied over from catalog_provider.py
class A2uiCatalogProvider(ABC):
  """Abstract base class for providing A2UI schemas and catalogs."""

  @abstractmethod
  def load(self) -> Dict[str, Any]:
    """Loads a catalog definition.

    Returns:
      The loaded catalog as a dictionary.
    """
    pass


# Catalog provider backed by a local file path.
# Copied over from catalog_provider.py
# Modified to set encoding to "utf-8"
class FileSystemCatalogProvider(A2uiCatalogProvider):
  def __init__(self, path: str):
    self.path = path

  def load(self) -> dict[str, Any]:
    with open(self.path, "r", encoding=ENCODING) as file:
      return json.load(file)


# Configuration describing one catalog source.
# Copied over from catalog.py
# Modified to have examples path resolving logic inplace (instead of using resolve_examples_path)
@dataclass
class CatalogConfig:
  name: str
  provider: A2uiCatalogProvider
  examples_path: Optional[str] = None

  @classmethod
  def from_path(
      cls,
      name: str,
      catalog_path: str,
      examples_path: Optional[str] = None,
  ) -> "CatalogConfig":
    parsed = urlparse(catalog_path)
    if not parsed.scheme or parsed.scheme == "file":
      provider = FileSystemCatalogProvider(parsed.path)
    else:
      raise ValueError(f"Unsupported catalog URL scheme: {catalog_path}")

    resolved_examples: Optional[str] = None
    if examples_path:
      parsed_examples = urlparse(examples_path)
      if not parsed_examples.scheme or parsed_examples.scheme == "file":
        resolved_examples = parsed_examples.path
      else:
        raise ValueError(f"Unsupported examples URL scheme: {examples_path}")

    return cls(name=name, provider=provider, examples_path=resolved_examples)


# Resolved catalog and schemas used for prompt generation and validation.
# Copied over from catalog.py
@dataclass(frozen=True)
class A2uiCatalog:
  version: str
  name: str
  catalog_schema: dict[str, Any]
  s2c_schema: dict[str, Any]
  common_types_schema: dict[str, Any]

  @property
  def catalog_id(self) -> str:
    if CATALOG_ID_KEY not in self.catalog_schema:
      raise ValueError(f"Catalog '{self.name}' missing catalogId")
    return self.catalog_schema[CATALOG_ID_KEY]

  @property
  def validator(self) -> "A2uiValidator":
    return A2uiValidator(self)

  def _with_pruned_components(self, allowed_components: list[str]) -> "A2uiCatalog":
    """Returns a new catalog with only allowed components.

    Args:
      allowed_components: List of component names to include.

    Returns:
      A copy of the catalog with only allowed components.
    """
    if not allowed_components:
      return self

    schema_copy = copy.deepcopy(self.catalog_schema)
    if CATALOG_COMPONENTS_KEY in schema_copy and isinstance(schema_copy[CATALOG_COMPONENTS_KEY], dict):
      all_comps = schema_copy[CATALOG_COMPONENTS_KEY]
      schema_copy[CATALOG_COMPONENTS_KEY] = {k: v for k, v in all_comps.items() if k in allowed_components}

    # Filter anyComponent oneOf if it exists
    # Path: $defs -> anyComponent -> oneOf
    if "$defs" in schema_copy and "anyComponent" in schema_copy["$defs"]:
      any_comp = schema_copy["$defs"]["anyComponent"]
      if "oneOf" in any_comp and isinstance(any_comp["oneOf"], list):
        filtered_one_of = []
        for item in any_comp["oneOf"]:
          if "$ref" in item:
            ref = item["$ref"]
            if ref.startswith(f"#/{CATALOG_COMPONENTS_KEY}/"):
              comp_name = ref.split("/")[-1]
              if comp_name in allowed_components:
                filtered_one_of.append(item)
            else:
              logging.warning("Skipping unknown ref format: %s", ref)
          else:
            logging.warning("Skipping non-ref item in anyComponent oneOf: %s", item)
        any_comp["oneOf"] = filtered_one_of

    return replace(self, catalog_schema=schema_copy)

  def _with_pruned_messages(self, allowed_messages: list[str]) -> "A2uiCatalog":
    """Returns a new catalog with only allowed messages.

    Args:
      allowed_messages: List of message names to include in s2c_schema.

    Returns:
      A copy of the catalog with only allowed messages.
    """
    if not allowed_messages:
      return self

    s2c_schema_copy = copy.deepcopy(self.s2c_schema)

    if self.version == VERSION_0_8:
      # 0.8 style: Messages are in root properties.
      if "properties" in s2c_schema_copy and isinstance(s2c_schema_copy["properties"], dict):
        s2c_schema_copy["properties"] = _prune_defs_by_reachability(
            defs=s2c_schema_copy["properties"],
            root_def_names=allowed_messages,
            internal_ref_prefix="#/properties/",
        )
    else:
      # 0.9+ style: Messages are in $defs and referenced via oneOf.
      if "oneOf" in s2c_schema_copy and isinstance(s2c_schema_copy["oneOf"], list):
        s2c_schema_copy["oneOf"] = [
            item
            for item in s2c_schema_copy["oneOf"]
            if "$ref" in item
            and isinstance(item["$ref"], str)
            and item["$ref"].startswith("#/$defs/")
            and item["$ref"].split("/")[-1] in allowed_messages
        ]

      if "$defs" in s2c_schema_copy and isinstance(s2c_schema_copy["$defs"], dict):
        s2c_schema_copy["$defs"] = _prune_defs_by_reachability(
            defs=s2c_schema_copy["$defs"],
            root_def_names=allowed_messages,
            internal_ref_prefix="#/$defs/",
        )

    return replace(self, s2c_schema=s2c_schema_copy)

  def _with_pruned_common_types(self) -> "A2uiCatalog":
    """Returns a new catalog with unused common types pruned from the schema."""
    if not self.common_types_schema or "$defs" not in self.common_types_schema:
      return self

    # Initialize roots with ONLY refs targeting common_types.json from external schemas
    external_refs = _collect_refs(self.catalog_schema)
    external_refs.update(_collect_refs(self.s2c_schema))

    root_common_types = []
    for ref in external_refs:
      if "common_types.json#/$defs/" in ref:
        root_common_types.append(ref.split("#/$defs/")[-1])

    new_common_types_schema = copy.deepcopy(self.common_types_schema)
    new_common_types_schema["$defs"] = _prune_defs_by_reachability(
        defs=new_common_types_schema["$defs"],
        root_def_names=root_common_types,
        internal_ref_prefix="#/$defs/",
    )

    return replace(self, common_types_schema=new_common_types_schema)

  def with_pruning(
      self,
      allowed_components: Optional[list[str]] = None,
      allowed_messages: Optional[list[str]] = None,
  ) -> "A2uiCatalog":
    """Returns a new catalog with pruned components and messages.

    Args:
      allowed_components: List of component names to include.
      allowed_messages: List of message names to include in s2c_schema.

    Returns:
      A copy of the catalog with pruned components and messages.
    """
    catalog = self
    if allowed_components:
      catalog = catalog._with_pruned_components(allowed_components)

    if allowed_messages:
      catalog = catalog._with_pruned_messages(allowed_messages)

    return catalog._with_pruned_common_types()

  def render_as_llm_instructions(self) -> str:
    """Renders the catalog and schema as LLM instructions."""
    all_schemas = []
    all_schemas.append(A2UI_SCHEMA_BLOCK_START)

    server_client_str = json.dumps(self.s2c_schema, separators=(",", ":")) if self.s2c_schema else "{}"
    all_schemas.append(f"### Server To Client Schema:\n{server_client_str}")

    if self.common_types_schema and "$defs" in self.common_types_schema and self.common_types_schema["$defs"]:
      common_str = json.dumps(self.common_types_schema, separators=(",", ":"))
      all_schemas.append(f"### Common Types Schema:\n{common_str}")

    catalog_str = json.dumps(self.catalog_schema, separators=(",", ":"))
    all_schemas.append(f"### Catalog Schema:\n{catalog_str}")

    all_schemas.append(A2UI_SCHEMA_BLOCK_END)

    return "\n\n".join(all_schemas)

  def load_examples(self, path: Optional[str], validate: bool = False) -> str:
    """Loads and validates examples from a directory or a glob pattern."""
    if not path:
      return ""

    # If it's a directory, support backward compatibility by appending /*.json
    if os.path.isdir(path):
      pattern = os.path.join(path, "*.json")
    else:
      pattern = path

    # Use glob to find files
    matched_files = glob.glob(pattern, recursive=True)

    if not matched_files:
      if not os.path.isdir(path) and not any(c in path for c in "*?[]"):
        logging.warning(
            f"Example path {path} is neither a directory nor a valid glob pattern"
        )
      return ""

    # Sort for determinism
    matched_files.sort()

    merged_examples = []
    for full_path in matched_files:
      if not os.path.isfile(full_path):
        continue

      basename = os.path.splitext(os.path.basename(full_path))[0]
      with open(full_path, "r", encoding=ENCODING) as file:
        content = file.read()

      if validate:
        self._validate_example(full_path, content)

      merged_examples.append(f"---BEGIN {basename}---\n{content}\n---END {basename}---")

    if not merged_examples:
      return ""
    return "\n\n".join(merged_examples)

  def _validate_example(self, full_path: str, content: str) -> None:
    try:
      json_data = json.loads(content)
      self.validator.validate(json_data)
    except Exception as error:
      raise ValueError(f"Failed to validate example {full_path}: {error}") from error


# --- Internal helpers --------------------------------------------------------

# Copied over from catalog.py
def _collect_refs(obj: Any) -> set[str]:
  """Recursively collects all $ref values from a JSON object."""
  refs = set()
  if isinstance(obj, dict):
    for k, v in obj.items():
      if k == "$ref" and isinstance(v, str):
        refs.add(v)
      else:
        refs.update(_collect_refs(v))
  elif isinstance(obj, list):
    for item in obj:
      refs.update(_collect_refs(item))
  return refs

# Copied over from catalog.py
def _prune_defs_by_reachability(
    defs: dict[str, Any],
    root_def_names: list[str],
    internal_ref_prefix: str = "#/$defs/",
) -> dict[str, Any]:
  """Prunes definitions not reachable from the provided roots.

  Args:
    defs: The dictionary of definitions to prune.
    root_def_names: The names of the definitions to start the traversal from.
    internal_ref_prefix: The prefix used for internal references.

  Returns:
    A new dictionary containing only reachable definitions.
  """
  visited_defs = set()
  refs_queue = collections.deque(root_def_names)

  while refs_queue:
    def_name = refs_queue.popleft()
    if def_name in defs and def_name not in visited_defs:
      visited_defs.add(def_name)

      internal_refs = _collect_refs(defs[def_name])
      for ref in internal_refs:
        if ref.startswith(internal_ref_prefix):
          refs_queue.append(ref.split(internal_ref_prefix)[-1])

  return {k: v for k, v in defs.items() if k in visited_defs}


# Custom helper to recompute derived v0.9 `$defs` unions from component/function keys.
# Args:
#   catalog: Catalog JSON containing `components` and optional `functions`.
# Returns:
#   None. Mutates `catalog` in place.
def _rebuild_catalog_defs(catalog: dict[str, Any]) -> None:
  components = catalog.get(CATALOG_COMPONENTS_KEY, {})
  functions = catalog.get("functions", {})
  defs = catalog.setdefault("$defs", {})

  any_component_refs = [{"$ref": f"#/components/{name}"} for name in sorted(components.keys())]
  defs["anyComponent"] = {"oneOf": any_component_refs} if any_component_refs else {"not": {}}

  any_function_refs = [{"$ref": f"#/functions/{name}"} for name in sorted(functions.keys())]
  defs["anyFunction"] = {"oneOf": any_function_refs} if any_function_refs else {"not": {}}


# Custom helper to merge basic and custom v0.9 catalogs with custom keys taking precedence.
# Args:
#   basic_catalog_schema: Base/bundled catalog schema.
#   custom_catalog_schema: Overlay catalog schema.
#   merged_catalog_id: Catalog id/$id assigned to merged output.
# Returns:
#   New merged catalog schema with rebuilt derived defs.
def _merge_catalog_schemas(
    basic_catalog_schema: dict[str, Any],
    custom_catalog_schema: dict[str, Any],
    merged_catalog_id: str,
) -> dict[str, Any]:
  merged = copy.deepcopy(basic_catalog_schema)
  merged[CATALOG_ID_KEY] = merged_catalog_id
  merged["$id"] = merged_catalog_id
  merged["title"] = "Merged A2UI Catalog"
  merged["description"] = "Merged basic and custom v0.9 component/function catalog."

  merged_components = merged.setdefault(CATALOG_COMPONENTS_KEY, {})
  merged_components.update(copy.deepcopy(custom_catalog_schema.get(CATALOG_COMPONENTS_KEY, {})))

  merged_functions = merged.setdefault("functions", {})
  merged_functions.update(copy.deepcopy(custom_catalog_schema.get("functions", {})))

  # Custom components may reference custom definitions (for example the
  # OAPopup position types). Preserve those definitions before rebuilding the
  # two derived union definitions below.
  merged_defs = merged.setdefault("$defs", {})
  merged_defs.update(copy.deepcopy(custom_catalog_schema.get("$defs", {})))

  _rebuild_catalog_defs(merged)
  return merged

# Copied over from utils.py
# LLM is instructed to generate a list of messages, so we wrap the bundled schema in an array.
def wrap_as_json_array(a2ui_schema: dict[str, Any]) -> dict[str, Any]:
  """Wraps the A2UI schema in an array object to support multiple parts.

  Args:
      a2ui_schema: The A2UI schema to wrap.

  Returns:
      The wrapped A2UI schema object.

  Raises:
      ValueError: If the A2UI schema is empty.
  """
  if not a2ui_schema:
    raise ValueError("A2UI schema is empty")
  return {"type": "array", "items": a2ui_schema}

# Copied over from validator.py
def _inject_additional_properties(
    schema: dict[str, Any],
    source_properties: dict[str, Any],
    mapping: dict[str, str] | None = None,
) -> tuple[dict[str, Any], set[str]]:
  """
  Recursively injects properties from source_properties into nodes with additionalProperties=True and sets additionalProperties=False.

  Args:
      schema: The target schema to traverse and patch.
      source_properties: A dictionary of top-level property groups (e.g., "components", "styles") from the source schema.

  Returns:
      A tuple containing:
      - The patched schema.
      - A set of keys from source_properties that were injected.
  """
  injected_keys: set[str] = set()
  key_mapping = mapping or {}

  def recursive_inject(obj: Any) -> Any:
    if isinstance(obj, dict):
      new_obj: dict[str, Any] = {}
      for key, value in obj.items():
        # If this node has additionalProperties=True, we inject the source properties
        if isinstance(value, dict) and value.get("additionalProperties") is True:
          source_key = key_mapping.get(key, key)
          if source_key in source_properties:
            injected_keys.add(source_key)
            new_node = dict(value)
            new_node["additionalProperties"] = False
            new_node["properties"] = {
                **new_node.get("properties", {}),
                **source_properties[source_key],
            }
            new_obj[key] = new_node
          else:
            # No matching source group, keep as is but recurse children
            new_obj[key] = recursive_inject(value)
        else:
          # Not a node with additionalProperties, recurse children
          new_obj[key] = recursive_inject(value)
      return new_obj
    if isinstance(obj, list):
      return [recursive_inject(item) for item in obj]
    return obj

  return recursive_inject(schema), injected_keys

# Copied over from validator.py
class A2uiValidator:
  def __init__(self, catalog: A2uiCatalog):
    self._catalog = catalog
    self.version = getattr(catalog, "version", VERSION_0_8)
    self._registry: Any = None
    self._validator = self._build_validator()

  def get_version(self) -> str:
    return self.version

  def _build_validator(self):
    if self.version == VERSION_0_8:
      return self._build_0_8_validator()
    return self._build_0_9_validator()

  def _bundle_0_8_schemas(self) -> dict[str, Any]:
    if not self._catalog.s2c_schema:
      return {}

    bundled = copy.deepcopy(self._catalog.s2c_schema)
    # Prepare catalog components and styles for injection
    source_properties: dict[str, Any] = {}
    catalog_schema = self._catalog.catalog_schema
    if catalog_schema:
      if CATALOG_COMPONENTS_KEY in catalog_schema:
        # Special mapping for v0.8: "components" -> "component"
        source_properties["component"] = catalog_schema[CATALOG_COMPONENTS_KEY]
      if CATALOG_STYLES_KEY in catalog_schema:
        source_properties[CATALOG_STYLES_KEY] = catalog_schema[CATALOG_STYLES_KEY]

    bundled, _ = _inject_additional_properties(bundled, source_properties)
    return bundled

  def _build_0_8_validator(self):
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    bundled_schema = self._bundle_0_8_schemas()
    full_schema = wrap_as_json_array(bundled_schema)

    base_uri = self._catalog.s2c_schema.get("$id", BASE_SCHEMA_URL)
    # Even in v0.8, we may have references to common_types.json or other files.
    common_types_uri = urljoin(base_uri, "common_types.json")
    resources = [
        (
            common_types_uri,
            Resource.from_contents(
                self._catalog.common_types_schema,
                default_specification=DRAFT202012,
            ),
        ),
        (
            "common_types.json",
            Resource.from_contents(
                self._catalog.common_types_schema,
                default_specification=DRAFT202012,
            ),
        ),
    ]
    self._registry = Registry().with_resources(resources)
    validator_schema = copy.deepcopy(full_schema)
    validator_schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return Draft202012Validator(validator_schema, registry=self._registry)

  def _build_0_9_validator(self):
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012

    full_schema = wrap_as_json_array(self._catalog.s2c_schema)
    base_uri = self._catalog.s2c_schema.get("$id", BASE_SCHEMA_URL)
    # v0.9 schemas (e.g. server_to_client.json) use relative references like
    # 'catalog.json#/$defs/anyComponent'. Since server_to_client.json has
    # $id: https://a2ui.org/specification/v0_9/server_to_client.json,
    # these resolve to https://a2ui.org/specification/v0_9/catalog.json.
    # We must register them using these absolute URIs.
    catalog_uri = urljoin(base_uri, "catalog.json")
    common_types_uri = urljoin(base_uri, "common_types.json")

    resources = [
        (
            common_types_uri,
            Resource.from_contents(
                self._catalog.common_types_schema,
                default_specification=DRAFT202012,
            ),
        ),
        (
            catalog_uri,
            Resource.from_contents(
                self._catalog.catalog_schema,
                default_specification=DRAFT202012,
            ),
        ),
        # Fallbacks for robustness
        (
            "catalog.json",
            Resource.from_contents(
                self._catalog.catalog_schema,
                default_specification=DRAFT202012,
            ),
        ),
        (
            "common_types.json",
            Resource.from_contents(
                self._catalog.common_types_schema,
                default_specification=DRAFT202012,
            ),
        ),
    ]
    # Also register the catalog ID if it's different from the catalog URI
    if self._catalog.catalog_id and self._catalog.catalog_id != catalog_uri:
      resources.append(
          (
              self._catalog.catalog_id,
              Resource.from_contents(
                  self._catalog.catalog_schema,
                  default_specification=DRAFT202012,
              ),
          )
      )

    self._registry = Registry().with_resources(resources)
    validator_schema = copy.deepcopy(full_schema)
    validator_schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    return Draft202012Validator(validator_schema, registry=self._registry)

  def validate(
      self,
      a2ui_json: dict[str, Any] | list[Any],
      root_id: Optional[str] = None,
      strict_integrity: bool = True,
  ) -> None:
    """Validates an A2UI messages against the schema."""
    messages = a2ui_json if isinstance(a2ui_json, list) else [a2ui_json]
    if self.version == VERSION_0_9:
      self._validate_0_9_custom(messages, root_id, strict_integrity)
    else:
      # Fallback to old behavior for v0.8
      errors = list(self._validator.iter_errors(messages))
      if errors:
        error = errors[0]
        msg = f"Validation failed: {error.message}"
        if error.context:
          msg += "\nContext failures:"
          for sub_error in error.context:
            msg += f"\n  - {sub_error.message}"
        raise ValueError(msg)

      for message in messages:
        if not isinstance(message, dict):
          continue
        components = None
        surface_id = None
        if "surfaceUpdate" in message:
          components = message["surfaceUpdate"].get(COMPONENTS)
          surface_id = message["surfaceUpdate"].get("surfaceId")

        if components:
          ref_map = self.extract_component_ref_fields()
          selected_root_id = self._find_root_id(messages, surface_id)
          self._validate_component_integrity(
              selected_root_id, components, ref_map, skip_root_check=not strict_integrity
          )
          self.analyze_topology(
              selected_root_id, components, ref_map, raise_on_orphans=strict_integrity
          )
        _validate_recursion_and_paths(message)

  def _validate_0_9_custom(
      self,
      messages: list[dict[str, Any]],
      root_id: Optional[str] = None,
      strict_integrity: bool = True,
  ) -> None:
    all_errors: list[str] = []
    for idx, message in enumerate(messages):
      if not isinstance(message, dict):
        all_errors.append(f"messages[{idx}]: Is not an object")
        continue

      if "createSurface" in message:
        val = self._get_sub_validator("CreateSurfaceMessage")
        all_errors.extend(self._get_formatted_errors(val, message, f"messages[{idx}]"))
      elif "updateComponents" in message:
        all_errors.extend(
            self._get_update_components_errors(message, f"messages[{idx}]")
        )
      elif "updateDataModel" in message:
        val = self._get_sub_validator("UpdateDataModelMessage")
        all_errors.extend(self._get_formatted_errors(val, message, f"messages[{idx}]"))
      elif "deleteSurface" in message:
        val = self._get_sub_validator("DeleteSurfaceMessage")
        all_errors.extend(self._get_formatted_errors(val, message, f"messages[{idx}]"))
      else:
        all_errors.append(f"messages[{idx}]: Unknown message type with keys {list(message.keys())}")

    if all_errors:
      raise ValueError("Validation failed:\n" + "\n".join(f"  - {error}" for error in all_errors))

    # Integrity checks
    for message in messages:
      if not isinstance(message, dict):
        continue
      components = None
      surface_id = None
      if "updateComponents" in message and isinstance(message["updateComponents"], dict):
        components = message["updateComponents"].get(COMPONENTS)
        surface_id = message["updateComponents"].get("surfaceId")
      if components:
        ref_map = self.extract_component_ref_fields()
        selected_root_id = root_id or self._find_root_id(messages, surface_id)
        self._validate_component_integrity(
            selected_root_id, components, ref_map, skip_root_check=not strict_integrity
        )
        self.analyze_topology(
            selected_root_id, components, ref_map, raise_on_orphans=strict_integrity
        )
      _validate_recursion_and_paths(message)

  def _get_sub_validator(self, def_name: str):
    from jsonschema import Draft202012Validator

    sub_schema = self._catalog.s2c_schema.get("$defs", {}).get(def_name)
    if not sub_schema:
      raise ValueError(f"Definition {def_name} not found in schema")
    return Draft202012Validator(sub_schema, registry=self._registry)

  def _get_formatted_errors(self, validator: Any, instance: Any, base_path: str) -> list[str]:
    errors = list(validator.iter_errors(instance))
    formatted = []
    for err in errors:
      path_str = ".".join(str(p) for p in err.path)
      full_path = f"{base_path}.{path_str}" if path_str else base_path
      message = err.message
      if (
            (
                "Unevaluated properties are not allowed" in message
                or "Additional properties are not allowed" in message
            )
            and "(" in message
            and ")" in message
      ):
        message = message[message.find("(") + 1 : message.rfind(")")]
      formatted.append(f"{full_path}: {message}")
    return formatted

  def _get_update_components_errors(self, message: dict[str, Any], path: str) -> list[str]:
    errors: list[str] = []
    if "version" not in message or message["version"] != "v0.9":
      errors.append(f"{path}: Invalid version, expected 'v0.9'")

    update_components = message.get("updateComponents")
    if not isinstance(update_components, dict):
      errors.append(f"{path}: Expected updateComponents to be an object")
      return errors
    if "surfaceId" not in update_components or not isinstance(update_components["surfaceId"], str):
      errors.append(f"{path}.updateComponents: Invalid or missing surfaceId")
    components = update_components.get(COMPONENTS)
    if not isinstance(components, list):
      errors.append(f"{path}.updateComponents: Expected components to be an array")
      return errors
    for idx, comp in enumerate(components):
      comp_id = comp.get(ID) if isinstance(comp, dict) else None
      comp_path = (
          f"{path}.updateComponents.components[id='{comp_id}']"
          if comp_id
          else f"{path}.updateComponents.components[{idx}]"
      )
      errors.extend(self._get_single_component_errors(comp, comp_path))
    return errors

  def _get_single_component_errors(self, comp: dict[str, Any], path: str) -> list[str]:
    from jsonschema import Draft202012Validator

    if not isinstance(comp, dict):
      return [f"{path}: Component is not an object"]
    comp_type = comp.get("component")
    if not comp_type:
      return [f"{path}: Missing 'component' field"]
    catalog = self._catalog.catalog_schema
    if not catalog or CATALOG_COMPONENTS_KEY not in catalog:
      return [f"{path}: Catalog schema or components missing"]
    comp_schema = catalog[CATALOG_COMPONENTS_KEY].get(comp_type)
    if not comp_schema:
      return [f"{path}: Unknown component: {comp_type}"]
    temp_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": f"catalog.json#/components/{comp_type}",
    }
    validator = Draft202012Validator(temp_schema, registry=self._registry)
    return self._get_formatted_errors(validator, comp, path)

  def extract_component_ref_fields(self) -> dict[str, tuple[set[str], set[str]]]:
    """
    Parses the catalog/schema to identify which component properties reference other components.
    Returns a map: { component_name: (set_of_single_ref_fields, set_of_list_ref_fields) }
    """
    ref_map = {}

    all_components = {}
    # Version aware extraction
    if self.version == VERSION_0_8:
      # Search for components in s2c schema properties
      try:
        # Try nested path: surfaceUpdate -> components -> items -> properties -> component -> properties
        s2c = self._catalog.s2c_schema or {}
        props = s2c.get("properties", {})

        # Might be in surfaceUpdate or beginRendering component definitions
        if "surfaceUpdate" in props:
          su = props["surfaceUpdate"].get("properties", {})
          if "components" in su:
            items = su["components"].get("items", {})
            if "properties" in items:
              comp_wrapper = items["properties"].get("component", {})
              all_components = comp_wrapper.get("properties", {})
      except Exception:
        logging.warning("Failed to extract component ref fields from v0.8 schema")

      # Also check catalog schema if available
      if not all_components and self._catalog.catalog_schema:
        all_components = self._catalog.catalog_schema.get(COMPONENTS, {})
    else:  # v0.9+
      # In v0.9, components are defined in the catalog itself
      all_components = self._catalog.catalog_schema.get(COMPONENTS, {})

    # Helper to check if a property schema looks like a ComponentId reference
    def is_component_id_ref(prop_schema: dict[str, Any]) -> bool:
      if not isinstance(prop_schema, dict):
        return False
      ref = prop_schema.get("$ref", "")
      if isinstance(ref, str) and (
          ref.endswith("ComponentId") or ref.endswith("child") or "/child" in ref
      ):
        return True

      # Inline check
      if (
          prop_schema.get("type") == "string"
          and prop_schema.get("title") == "ComponentId"
      ):
        return True

      # Check oneOf/anyOf for refs
      for key in ["oneOf", "anyOf", "allOf"]:
        if key in prop_schema:
          for sub in prop_schema[key]:
            if is_component_id_ref(sub):
              return True
      return False

    def is_child_list_ref(prop_schema: dict[str, Any]) -> bool:
      if not isinstance(prop_schema, dict):
        return False
      ref = prop_schema.get("$ref", "")
      if isinstance(ref, str) and (
          ref.endswith("ChildList") or ref.endswith("children") or "/children" in ref
      ):
        return True

      # Inline check
      if prop_schema.get("type") == "object":
        props = prop_schema.get("properties", {})
        if "explicitList" in props or "template" in props or "componentId" in props:
          return True

      # Or array of ComponentIds
      if prop_schema.get("type") == "array":
        items = prop_schema.get("items", {})
        if is_component_id_ref(items):
          return True

      # Check oneOf/anyOf for refs
      for key in ["oneOf", "anyOf", "allOf"]:
        if key in prop_schema:
          for sub in prop_schema[key]:
            if is_child_list_ref(sub):
              return True
      return False

    for comp_name, comp_schema in all_components.items():
      single_refs = set()
      list_refs = set()

      def extract_from_props(cs: dict[str, Any]):
        if not isinstance(cs, dict):
          return
        props = cs.get("properties", {})
        for prop_name, prop_schema in props.items():
          if is_component_id_ref(prop_schema) or prop_name in [
              "child",
              "contentChild",
              "entryPointChild",
          ]:
            single_refs.add(prop_name)
          elif is_child_list_ref(prop_schema) or prop_name == "children":
            list_refs.add(prop_name)

        # Recurse into allOf/oneOf for properties
        for key in ["allOf", "oneOf", "anyOf"]:
          if key in cs:
            for sub in cs[key]:
              extract_from_props(sub)

      extract_from_props(comp_schema)

      if single_refs or list_refs:
        ref_map[comp_name] = (single_refs, list_refs)

    return ref_map

  def get_component_references(
      self, component: dict[str, Any], ref_fields_map: dict[str, tuple[set[str], set[str]]]
  ) -> Iterator[tuple[str, str]]:
    """
    Helper to extract all referenced component IDs from a component.
    Yields (referenced_id, field_name).
    """
    # Support both v0.8 and v0.9+
    comp_val = component.get("component")
    if isinstance(comp_val, str):
      # v0.9 flattened
      yield from self.get_refs_recursively(comp_val, component, ref_fields_map)
    elif isinstance(comp_val, dict):
      # v0.8 structured
      for comp_type, comp_props in comp_val.items():
        if isinstance(comp_props, dict):
          yield from self.get_refs_recursively(comp_type, comp_props, ref_fields_map)

  def get_refs_recursively(
      self,
      comp_type: str,
      props: dict[str, Any],
      ref_fields_map: dict[str, tuple[set[str], set[str]]],
  ) -> Iterator[tuple[str, str]]:
    """
    Helper to recursively extract component references from component props.
    Yields (referenced_id, field_name).
    """
    if not comp_type or not isinstance(props, dict):
      return
    single_refs, list_refs = ref_fields_map.get(comp_type, (set(), set()))

    # Standard A2UI reference fields to check as heuristics if not explicitly mapped
    HEURISTIC_SINGLE = {
        "child",
        "contentChild",
        "entryPointChild",
        "detail",
        "summary",
        "root",
    }
    HEURISTIC_LIST = {"children", "explicitList", "template"}

    for key, value in props.items():
      is_ref = False
      if key in single_refs or key in HEURISTIC_SINGLE:
        if isinstance(value, str):
          yield value, key
          is_ref = True
        elif isinstance(value, dict) and "componentId" in value:
          yield value["componentId"], f"{key}.componentId"
          is_ref = True
      elif key in list_refs or key in HEURISTIC_LIST:
        if isinstance(value, list):
          for item in value:
            if isinstance(item, str):
              yield item, key
              is_ref = True
        elif isinstance(value, dict):
          if "explicitList" in value:
            for item in value["explicitList"]:
              if isinstance(item, str):
                yield item, f"{key}.explicitList"
                is_ref = True
          elif "template" in value and isinstance(value["template"], dict):
            template = value["template"]
            if "componentId" in template:
              yield template["componentId"], f"{key}.template.componentId"
              is_ref = True
          elif "componentId" in value:
            yield value["componentId"], f"{key}.componentId"
            is_ref = True
      # Special handling for 'tabs' or other nested arrays
      if isinstance(value, list) and key not in list_refs:
        for idx, item in enumerate(value):
          if isinstance(item, dict):
            child_id = item.get("child")
            if isinstance(child_id, str):
              yield child_id, f"{key}[{idx}].child"
            component_id = item.get("componentId")
            if isinstance(component_id, str):
              yield component_id, f"{key}[{idx}].componentId"

  def _find_root_id(self, messages: list[dict[str, Any]], surface_id: Optional[str] = None) -> Optional[str]:
    """
    Finds the root id from a list of A2UI messages for a given surface.
    - For v0.8, the root id is in the beginRendering message.
    - For v0.9+, the root id is 'root'.
    """
    for message in messages:
      if not isinstance(message, dict):
        continue
      if "beginRendering" in message:
        begin = message["beginRendering"]
        if surface_id and begin.get("surfaceId") != surface_id:
          continue
        return begin.get(ROOT, ROOT)
      if "createSurface" in message:
        create_surface = message["createSurface"]
        if surface_id and create_surface.get("surfaceId") != surface_id:
          continue
        return ROOT
    return None

  def _validate_component_integrity(
      self,
      root_id: Optional[str],
      components: list[dict[str, Any]],
      ref_fields_map: dict[str, tuple[set[str], set[str]]],
      skip_root_check: bool = False,
  ) -> None:
    """
    Validates that:
    1. All component IDs are unique.
    2. A 'root' component exists.
    3. All references point to existing IDs.
    """
    ids: set[str] = set()
    # 1. Collect IDs and check for duplicates
    for comp in components:
      comp_id = comp.get(ID)
      if comp_id is None:
        continue
      if comp_id in ids:
        raise ValueError(f"Duplicate component ID: {comp_id}")
      ids.add(comp_id)
    # 2. Check for root component
    if not skip_root_check and root_id is not None and root_id not in ids:
      raise ValueError(f"Missing root component: No component has id='{root_id}'")
    # 3. Check for dangling references using helper
    # In an incremental update (root_id is None), components may reference IDs already on the client.
    if root_id is not None and not skip_root_check:
      for comp in components:
        for ref_id, field_name in self.get_component_references(comp, ref_fields_map):
          if ref_id not in ids:
            raise ValueError(
                f"Component '{comp.get(ID)}' references non-existent component '{ref_id}' in field '{field_name}'"
            )

  def analyze_topology(
      self,
      root_id: Optional[str],
      components: list[dict[str, Any]],
      ref_fields_map: dict[str, tuple[set[str], set[str]]],
      raise_on_orphans: bool = False,
  ) -> set[str]:
    """
    Analyzes the topology of the component tree and returns reachable component IDs.

    Args:
        root_id: The ID of the root component.
        components: The list of components.
        ref_fields_map: Map of component reference fields.
        raise_on_orphans: If True, raises ValueError if any components are unreachable from root.

    Returns:
        A set of reachable component IDs.

    Raises:
        ValueError: On circular references or self-references.
    """
    adj_list: dict[str, list[str]] = {}
    all_ids: set[str] = set()
    # Build Adjacency List
    for comp in components:
      comp_id = comp.get(ID)
      if comp_id is None:
        continue
      all_ids.add(comp_id)
      adj_list.setdefault(comp_id, [])
      for ref_id, field_name in self.get_component_references(comp, ref_fields_map):
        if ref_id == comp_id:
          raise ValueError(
              f"Self-reference detected: Component '{comp_id}' references itself in field '{field_name}'"
          )
        adj_list[comp_id].append(ref_id)

    visited: set[str] = set()
    recursion_stack: set[str] = set()

    # Detect Cycles and Depth using DFS
    def dfs(node_id: str, depth: int) -> None:
      if depth > MAX_GLOBAL_DEPTH:
        raise ValueError(f"Global recursion limit exceeded: logical depth > {MAX_GLOBAL_DEPTH}")
      visited.add(node_id)
      recursion_stack.add(node_id)
      for neighbor in adj_list.get(node_id, []):
        if neighbor not in visited:
          dfs(neighbor, depth + 1)
        elif neighbor in recursion_stack:
          raise ValueError(f"Circular reference detected involving component '{neighbor}'")
      recursion_stack.remove(node_id)

    if root_id is not None:
      if root_id in all_ids:
        dfs(root_id, 0)
      # Check for Orphans if requested
      if raise_on_orphans:
        orphans = all_ids - visited
        if orphans:
          sorted_orphans = sorted(list(orphans))
          raise ValueError(
              f"Component '{sorted_orphans[0]}' is not reachable from '{root_id}'"
          )
    else:
      # No root provided (e.g. partial update): we traverse everything to check for cycles
      for node_id in sorted(list(all_ids)):
        if node_id not in visited:
          dfs(node_id, 0)
    return visited

def _validate_recursion_and_paths(data: Any) -> None:
  """
  Validates:
  1. Global recursion depth limit (50).
  2. FunctionCall recursion depth limit (5).
  3. Path syntax for DataBindings/DataModelUpdates.
  """

  def traverse(item: Any, global_depth: int, func_depth: int):
    if global_depth > MAX_GLOBAL_DEPTH:
      raise ValueError(f"Global recursion limit exceeded: Depth > {MAX_GLOBAL_DEPTH}")

    if isinstance(item, list):
      for x in item:
        traverse(x, global_depth + 1, func_depth)
      return

    if isinstance(item, dict):
      # Check for path
      if PATH in item and isinstance(item[PATH], str):
        path = item[PATH]
        if not re.fullmatch(RELAXED_PATH_PATTERN, path):
          raise ValueError(f"Invalid path syntax: '{path}'")

      # Check for FunctionCall
      is_func = CALL in item and ARGS in item

      if is_func:
        if func_depth >= MAX_FUNC_CALL_DEPTH:
          raise ValueError(
              f"Recursion limit exceeded: {FUNCTION_CALL} depth > {MAX_FUNC_CALL_DEPTH}"
          )

        # Increment func_depth only for 'args', but global_depth matches traversal
        for k, v in item.items():
          if k == ARGS:
            traverse(v, global_depth + 1, func_depth + 1)
          else:
            traverse(v, global_depth + 1, func_depth)
      else:
        for v in item.values():
          traverse(v, global_depth + 1, func_depth)

  traverse(data, 0, 0)


# Copied from manager.py
# Modified to support merging separate schemas
# Schema manager supporting A2UI v0.8 and v0.9.
class A2uiSchemaManager(InferenceStrategy):
  # Initialize manager and load all version-specific schema/catalog assets.
  #
  #     Args:
  #       version: Target A2UI protocol version ("0.8" or "0.9").
  #       catalogs: Optional explicit catalog configuration overrides.
  #       accepts_inline_catalogs: Whether client inline catalogs are accepted.
  #       schema_modifiers: Optional schema transform hooks applied after JSON load.
  #
  def __init__(
      self,
      version: str = SUPPORTED_VERSION,
      catalogs: Optional[list[CatalogConfig]] = None,
      accepts_inline_catalogs: bool = False,
      schema_modifiers: Optional[list[Callable[[dict[str, Any]], dict[str, Any]]]] = None,
  ):
    if version not in SUPPORTED_VERSIONS:
      raise ValueError(
          f"Unsupported A2UI version '{version}'. Supported versions: {SUPPORTED_VERSIONS}."
      )

    self.version = version
    self._accepts_inline_catalogs = accepts_inline_catalogs
    self._schema_modifiers = schema_modifiers or []
    self._supported_catalogs: list[A2uiCatalog] = []
    self._catalog_example_paths: dict[str, str] = {}
    self.server_to_client_schema: dict[str, Any] = {}
    self.common_types_schema: dict[str, Any] = {}

    self._load_schemas(version, catalogs or [])

    if self.version == VERSION_0_8 and self._supported_catalogs:
      self.resolved_schema = copy.deepcopy(self._supported_catalogs[0].s2c_schema)

  # Copied from manager.py
  @property
  def accepts_inline_catalogs(self) -> bool:
    return self._accepts_inline_catalogs

  # Copied from manager.py
  @property
  def supported_catalog_ids(self) -> list[str]:
    return [catalog.catalog_id for catalog in self._supported_catalogs]

  # Copied from manager.py
  def _apply_modifiers(self, schema: dict[str, Any]) -> dict[str, Any]:
    if self._schema_modifiers:
      for modifier in self._schema_modifiers:
        schema = modifier(schema)
    return schema

  # Copied from manager.py
  # Modified to load the schema JSONs from file paths
  def _load_schemas(
      self,
      version: str,
      catalogs: Optional[list[CatalogConfig]] = None,
  ) -> None:
    """Loads separate schema components and processes catalogs."""
    catalogs = catalogs or []

    self.server_to_client_schema = self._apply_modifiers(self._load_json(DEFAULT_SERVER_SCHEMA_FILE))
    self.common_types_schema = {}

    if version == VERSION_0_9:
      self.common_types_schema = self._apply_modifiers(self._load_json(DEFAULT_COMMON_TYPES_FILE))

      basic_raw = self._apply_modifiers(self._load_json(DEFAULT_BASIC_CATALOG_FILE))
      custom_raw = self._apply_modifiers(self._load_json(DEFAULT_COMPLETE_CATALOG_FILE))

      self.basic_catalog_schema = basic_raw
      self.custom_catalog_schema = custom_raw

      merged_schema = _merge_catalog_schemas(
          self.basic_catalog_schema,
          self.custom_catalog_schema,
          V09_CATALOG_ID,
      )
      self.default_catalog = self._catalog_from_schema(DEFAULT_CATALOG_NAME, merged_schema)
    elif version == VERSION_0_8:
      self.basic_catalog_schema = self._apply_modifiers(self._load_json(DEFAULT_BASIC_CATALOG_FILE))
      self.custom_catalog_schema = self._apply_modifiers(self._load_json(DEFAULT_CUSTOM_CATALOG_FILE))
      self.resolved_schema = self._build_resolved_schema(
          self.basic_catalog_schema,
          self.custom_catalog_schema,
      )
      self.default_catalog = self._catalog_from_schema(
          DEFAULT_CATALOG_NAME,
          {
              CATALOG_ID_KEY: V08_CATALOG_ID,
              CATALOG_COMPONENTS_KEY: (
                  self.resolved_schema["properties"]["surfaceUpdate"]["properties"]["components"]["items"][
                      "properties"
                  ]["component"]["properties"]
              ),
          },
      )
    else:
      raise ValueError(f"Unsupported manager version in initialization: {version}")

    self._initialize_supported_catalogs(catalogs)

  # Custom helper to load one JSON file from the active version directory.
  # Args:
  #   file_name: File name under `v{version}/`.
  # Returns:
  #   Parsed JSON object.
  def _load_json(self, file_name: str) -> dict[str, Any]:
    full_path = (
        Path(__file__).resolve().parent
        / f"v{self.version.replace('.', '_')}"
        / file_name
    )
    with open(full_path, "r", encoding=ENCODING) as file:
      return json.load(file)

  def _load_text(self, file_name: str) -> str:
    manager_path = Path(__file__).resolve()
    for frame in inspect.stack()[1:]:
      frame_path = Path(frame.filename).resolve()
      if frame_path == manager_path:
        continue
      local_path = frame_path.with_name(file_name)
      if local_path.exists():
        with open(local_path, "r", encoding=ENCODING) as file:
          return file.read().strip()

    return ""

  # Custom helper to extract and validate the v0.8 `components` object from a catalog.
  # Args:
  #   catalog_schema: Catalog schema dict.
  #   catalog_name: Human-readable label used in error messages.
  # Returns:
  #   Components mapping.
  # Raises:
  #   ValueError: If `components` is missing or not an object.
  def _extract_components(
      self,
      catalog_schema: dict[str, Any],
      catalog_name: str,
  ) -> dict[str, Any]:
    components = catalog_schema.get(CATALOG_COMPONENTS_KEY)
    if not isinstance(components, dict):
      raise ValueError(f"{catalog_name} schema must contain an object at key 'components'.")
    return components

  # Custom helper to build resolved v0.8 server schema with merged component definitions.
  # Args:
  #   basic_catalog_schema: Base component catalog schema.
  #   custom_catalog_schema: Custom component catalog schema.
  # Returns:
  #   Deep-copied server schema patched with merged component wrapper props.
  def _build_resolved_schema(
      self,
      basic_catalog_schema: dict[str, Any],
      custom_catalog_schema: dict[str, Any],
  ) -> dict[str, Any]:
    basic_components = self._extract_components(basic_catalog_schema, "basic_catalog")
    custom_components = self._extract_components(custom_catalog_schema, "custom_catalog")
    merged_components = {**basic_components, **custom_components}
    schema = copy.deepcopy(self.server_to_client_schema)
    component_wrapper = (
        schema["properties"]["surfaceUpdate"]["properties"]["components"]["items"]["properties"]["component"]
    )
    component_wrapper["additionalProperties"] = False
    component_wrapper["minProperties"] = 1
    component_wrapper["maxProperties"] = 1
    component_wrapper["properties"] = merged_components
    return schema

  # Custom helper to construct a version-correct `A2uiCatalog` from raw catalog JSON.
  # Args:
  #   name: Catalog display/internal name.
  #   catalog_schema: Raw catalog schema dict.
  # Returns:
  #   `A2uiCatalog` with version-appropriate `s2c_schema/common_types_schema`.
  # Raises:
  #   ValueError: If manager version is unsupported.
  def _catalog_from_schema(
      self,
      name: str,
      catalog_schema: dict[str, Any],
  ) -> A2uiCatalog:
    if self.version == VERSION_0_8:
      components = self._extract_components(catalog_schema, name)
      s2c = copy.deepcopy(self.server_to_client_schema)
      wrapper = (
          s2c["properties"]["surfaceUpdate"]["properties"]["components"]["items"]["properties"]["component"]
      )
      wrapper["additionalProperties"] = False
      wrapper["minProperties"] = 1
      wrapper["maxProperties"] = 1
      wrapper["properties"] = components
      return A2uiCatalog(
          version=self.version,
          name=name,
          catalog_schema=copy.deepcopy(catalog_schema),
          s2c_schema=s2c,
          common_types_schema={},
      )

    return A2uiCatalog(
        version=self.version,
        name=name,
        catalog_schema=copy.deepcopy(catalog_schema),
        s2c_schema=copy.deepcopy(self.server_to_client_schema),
        common_types_schema=copy.deepcopy(self.common_types_schema),
    )

  # Custom helper to initialize agent-supported catalogs from config or built-in defaults.
  # Args:
  #   catalogs: Optional explicit catalog configurations.
  # Returns:
  #   None. Populates `_supported_catalogs` and `_catalog_example_paths`.
  # Raises:
  #   ValueError: If manager version is unsupported.
  def _initialize_supported_catalogs(self, catalogs: list[CatalogConfig]) -> None:
    if catalogs:
      for config in catalogs:
        raw_catalog = config.provider.load()
        if self.version == "0.9":
          catalog_schema = self._apply_modifiers(raw_catalog)
        elif self.version == VERSION_0_8:
          catalog_schema = self._apply_modifiers(raw_catalog)
        else:
          raise ValueError(f"Unsupported manager version for catalog init: {self.version}")
        catalog = self._catalog_from_schema(config.name, catalog_schema)
        self._supported_catalogs.append(catalog)
        if config.examples_path:
          self._catalog_example_paths[catalog.catalog_id] = config.examples_path
      return

    self._supported_catalogs.append(self.default_catalog)
    if self.version == "0.9":
      self._supported_catalogs.append(self._catalog_from_schema(BASIC_CATALOG_NAME, self.basic_catalog_schema))
      self._supported_catalogs.append(self._catalog_from_schema(CUSTOM_CATALOG_NAME, self.custom_catalog_schema))
    elif self.version != VERSION_0_8:
      raise ValueError(f"Unsupported manager version for supported catalogs: {self.version}")

  # Custom helper to merge client inline catalog fragments over a selected base catalog.
  # Args:
  #   base_catalog: Agent-selected base catalog to start from.
  #   inline_catalogs: Client-provided inline catalog fragment list.
  # Returns:
  #   New merged catalog wrapped as an `A2uiCatalog`.
  # Raises:
  #   ValueError: On malformed base/inline shapes or unsupported versions.
  def _merge_inline_catalog(
      self,
      base_catalog: A2uiCatalog,
      inline_catalogs: list[dict[str, Any]],
  ) -> A2uiCatalog:
    merged_catalog_schema = copy.deepcopy(base_catalog.catalog_schema)
    merged_components = merged_catalog_schema.setdefault(CATALOG_COMPONENTS_KEY, {})
    if not isinstance(merged_components, dict):
      raise ValueError("Base catalog components must be an object")

    merged_functions = merged_catalog_schema.setdefault("functions", {})
    if self.version == "0.9":
      if not isinstance(merged_functions, dict):
        raise ValueError("Base catalog functions must be an object")

    for inline_catalog in inline_catalogs:
      if self.version == "0.9":
        inline_schema = self._apply_modifiers(copy.deepcopy(inline_catalog))
      else:
        inline_schema = self._apply_modifiers(copy.deepcopy(inline_catalog))
      inline_components = inline_schema.get(CATALOG_COMPONENTS_KEY, {})
      if isinstance(inline_components, dict):
        merged_components.update(inline_components)
      if self.version == "0.9":
        inline_functions = inline_schema.get("functions", {})
        if isinstance(inline_functions, dict):
          merged_functions.update(inline_functions)

    if self.version == "0.9":
      _rebuild_catalog_defs(merged_catalog_schema)
    return self._catalog_from_schema(INLINE_CATALOG_NAME, merged_catalog_schema)

  # Copied from manager.py
  def _select_catalog(
      self,
      client_ui_capabilities: Optional[dict[str, Any]] = None,
  ) -> A2uiCatalog:
    """Selects the component catalog for the prompt based on client capabilities.

    Selection priority:
    1. If inline catalogs are provided (and accepted by the agent), their
       components are merged on top of a base catalog. The base is determined
       by supportedCatalogIds (if also provided) or the agent's default catalog.
    2. If only supportedCatalogIds is provided, pick the first mutually
       supported catalog.
    3. Fallback to the first agent-supported catalog (usually the bundled catalog).

    Args:
      client_ui_capabilities: A dictionary of client UI capabilities, containing
        inline catalogs and client-supported catalog IDs.

    Returns:
      The resolved A2uiCatalog.
    Raises:
      ValueError: If inline catalogs are sent but not accepted, or if no
        mutually supported catalog is found.
    """
    if not self._supported_catalogs:
      raise ValueError("No supported catalogs found.")  # This should not happen.

    if not client_ui_capabilities or not isinstance(client_ui_capabilities, dict):
      return self._supported_catalogs[0]

    inline_catalogs: list[dict[str, Any]] = client_ui_capabilities.get(
        INLINE_CATALOGS_KEY, []
    )
    client_supported_catalog_ids: list[str] = client_ui_capabilities.get(
        SUPPORTED_CATALOG_IDS_KEY, []
    )

    if inline_catalogs and not self._accepts_inline_catalogs:
      raise ValueError(
          f"Inline catalog '{INLINE_CATALOGS_KEY}' is provided in client UI"
          " capabilities. However, the agent does not accept inline catalogs."
      )

    if inline_catalogs:
      # Determine the base catalog: use supportedCatalogIds if provided,
      # otherwise fall back to the agent's default catalog.
      base_catalog = self._supported_catalogs[0]
      if client_supported_catalog_ids:
        agent_supported_catalogs = {c.catalog_id: c for c in self._supported_catalogs}
        for cscid in client_supported_catalog_ids:
          if cscid in agent_supported_catalogs:
            base_catalog = agent_supported_catalogs[cscid]
            break

      return self._merge_inline_catalog(base_catalog, inline_catalogs)

    if not client_supported_catalog_ids:
      return self._supported_catalogs[0]

    agent_supported_catalogs = {c.catalog_id: c for c in self._supported_catalogs}
    for cscid in client_supported_catalog_ids:
      if cscid in agent_supported_catalogs:
        return agent_supported_catalogs[cscid]

    raise ValueError(
        "No client-supported catalog found on the agent side. Agent-supported catalogs"
        f" are: {[c.catalog_id for c in self._supported_catalogs]}"
    )

  # Copied from manager.py
  def get_selected_catalog(
      self,
      client_ui_capabilities: Optional[dict[str, Any]] = None,
      allowed_components: Optional[list[str]] = None,
      allowed_messages: Optional[list[str]] = None,
  ) -> A2uiCatalog:
    """Gets the selected catalog after selection and component pruning."""
    catalog = self._select_catalog(client_ui_capabilities)
    pruned_catalog = catalog.with_pruning(allowed_components, allowed_messages)
    return pruned_catalog

  # Copied from manager.py
  def load_examples(self, catalog: A2uiCatalog, validate: bool = False) -> str:
    """Loads examples for a catalog."""
    if catalog.catalog_id in self._catalog_example_paths:
      return catalog.load_examples(
          self._catalog_example_paths[catalog.catalog_id], validate=validate
      )
    return ""

  # Copied from manager.py
  def generate_system_prompt(
      self,
      role_description: str,
      workflow_description: str = "",
      ui_description: str = "",
      client_ui_capabilities: Optional[dict[str, Any]] = None,
      allowed_components: Optional[list[str]] = None,
      allowed_messages: Optional[list[str]] = None,
      include_schema: bool = False,
      include_rules: bool = False,
      include_examples: bool = False,
      validate_examples: bool = False,
  ) -> str:
    """Assembles the final system instruction for the LLM."""
    parts = [role_description]

    workflow = DEFAULT_WORKFLOW_RULES_BY_VERSION[self.version]
    if workflow_description:
      workflow += f"\n{workflow_description}"
    parts.append(f"## Workflow Description:\n{workflow}")

    if ui_description:
      parts.append(f"## UI Description:\n{ui_description}")

    # Custom v0.9 integration for auxiliary rules supplied in a sibling rules.txt.
    if include_rules:
      catalog_rules = self._load_text(DEFAULT_CATALOG_RULES_FILE)
      if catalog_rules:
        parts.append(f"## Catalog Rules:\n{catalog_rules}")

    selected_catalog = self.get_selected_catalog(
        client_ui_capabilities, allowed_components, allowed_messages
    )

    if include_schema:
      parts.append(selected_catalog.render_as_llm_instructions())

    if include_examples:
      examples_str = self.load_examples(selected_catalog, validate=validate_examples)
      if examples_str:
        parts.append(f"### Examples:\n{examples_str}")

    return "\n\n".join(parts)


__all__ = [
    "A2UI_OPEN_TAG",
    "A2UI_CLOSE_TAG",
    "A2UI_SCHEMA_BLOCK_START",
    "A2UI_SCHEMA_BLOCK_END",
    "SUPPORTED_VERSIONS",
    "SUPPORTED_VERSION",
    "SUPPORTED_VERSION_KEY",
    "VERSION_0_8",
    "CATALOG_ID_KEY",
    "CATALOG_COMPONENTS_KEY",
    "ENCODING",
    "DEFAULT_CATALOG_NAME",
    "DEFAULT_CATALOG_ID",
    "INLINE_CATALOG_NAME",
    "SUPPORTED_CATALOG_IDS_KEY",
    "INLINE_CATALOGS_KEY",
    "DEFAULT_SERVER_SCHEMA_FILE",
    "DEFAULT_COMMON_TYPES_FILE",
    "DEFAULT_BASIC_CATALOG_FILE",
    "DEFAULT_COMPLETE_CATALOG_FILE",
    "DEFAULT_CUSTOM_CATALOG_FILE",
    "DEFAULT_CATALOG_RULES_FILE",
    "MAX_GLOBAL_DEPTH",
    "MAX_FUNC_CALL_DEPTH",
    "RELAXED_PATH_PATTERN",
    "DEFAULT_WORKFLOW_RULES_V08",
    "DEFAULT_WORKFLOW_RULES_V09",
    "DEFAULT_WORKFLOW_RULES_BY_VERSION",
    "DEFAULT_WORKFLOW_RULES",
    "InferenceStrategy",
    "A2uiCatalogProvider",
    "FileSystemCatalogProvider",
    "CatalogConfig",
    "A2uiCatalog",
    "A2uiValidator",
    "A2uiSchemaManager",
]
