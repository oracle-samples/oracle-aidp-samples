"""Workspace ``.py`` modules that exported notebooks import from.

The saved (migrated) notebook runs with its folder and each parent folder up
to the export root on ``sys.path``, nearest first (see
``job_migrate.build_artifact_workspace_bootstrap_cell``), so
``from imports.constants import *`` binds names defined in an exported module.
The data check resolves those modules the same way, without executing them,
so a read whose path comes from such a constant is checked instead of being
invisible.  Migration-time execution orders the same folders outermost first,
so a module name found in more than one of them is reported, not resolved;
so is an import whose lookup depends on a folder that could not be listed.
Module files and folder listings come from caller-supplied readers; this
module performs no I/O of its own.
"""
import posixpath
import re
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from data_check_extract import ModuleNamespace, module_namespace, python_import_froms

# dbx_export lays exported notebooks out as
# <export_output>/notebooks/{Users,Repos,Shared,GIT}/... (dbx_export.sanitizer);
# GIT/ holds Git repository notebooks, which the saved notebook runs from their
# own folder.
_EXPORT_LAYOUT_RE = re.compile(r"^(/Workspace/.+?)/notebooks/(Users|Repos|Shared|GIT)/")
MAX_IMPORT_DEPTH = 5

# (file names, folder names) directly under a folder; None if it is absent.
Listing = Optional[Tuple[FrozenSet[str], FrozenSet[str]]]
# Imported module name -> its namespace, or None if it could not be analysed.
Namespaces = Dict[str, Optional[ModuleNamespace]]


def module_search_dirs(notebook_path: str) -> List[str]:
    """Folders searched for imports, nearest first, as the saved notebook
    orders sys.path."""
    match = _EXPORT_LAYOUT_RE.match(notebook_path)
    if not match:
        return []
    root = match.group(1)
    dirs = []
    current = posixpath.dirname(notebook_path)
    while current.startswith(root):
        dirs.append(current)
        if current == root:
            break
        current = posixpath.dirname(current)
    return dirs


def repository_notebook_dir(notebook_path: str) -> Optional[str]:
    """Run-time working directory of a Git repository notebook, else None."""
    match = _EXPORT_LAYOUT_RE.match(notebook_path)
    if match and match.group(2) == "GIT":
        return posixpath.dirname(notebook_path)
    return None


def _loaded_file(files: FrozenSet[str], stem: str) -> Optional[str]:
    """The file CPython's FileFinder loads for ``stem`` in one folder, in its
    loader order: extension module, ``.py`` source, ``.pyc`` bytecode."""
    extensions = sorted(
        name for name in files
        if name.startswith(stem + ".") and name.endswith((".so", ".pyd"))
    )
    if extensions:
        return extensions[0]
    for suffix in (".py", ".pyc"):
        if stem + suffix in files:
            return stem + suffix
    return None


def _kind(file_name: str) -> str:
    return "source" if file_name.endswith(".py") else "unsupported"


def _module_notebook(source: str) -> Dict[str, Any]:
    return {
        "metadata": {"language_info": {"name": "python"}},
        "cells": [{"cell_type": "code", "source": [source]}],
    }


class ImportedModuleSymbols:
    """Resolve the workspace modules a notebook imports from.

    The result is a namespace per imported workspace module; the analyser
    applies each ``from ... import`` at its own statement position, so a read
    sees the binding active where it runs. ``list_entries(folder)`` returns a
    `Listing` and raises on a failed listing; ``read_file(path)`` returns the
    file bytes and raises on failure. Listings and module namespaces are
    cached for the lifetime of the instance, so notebooks of one job share
    the lookups.
    """

    def __init__(self, list_entries: Callable[[str], Listing],
                 read_file: Callable[[str], bytes]):
        self._list_entries = list_entries
        self._read_file = read_file
        self._listings: Dict[str, Listing] = {}
        self._listing_errors: Dict[str, str] = {}
        self._modules: Dict[Tuple[str, Tuple[str, ...]],
                            Tuple[Optional[ModuleNamespace], List[Dict[str, Any]]]] = {}

    def for_notebook(self, notebook: Dict[str, Any], notebook_path: str
                     ) -> Tuple[Namespaces, List[Dict[str, Any]]]:
        """Return (module namespaces, not-analyzed gaps) for one notebook.

        A module maps to None when it exists in the workspace but could not
        be analysed; installed packages are absent from the mapping.
        """
        dirs = tuple(module_search_dirs(notebook_path))
        gaps: List[Dict[str, Any]] = []
        if not dirs:
            return {}, gaps
        return self._namespaces(python_import_froms(notebook), dirs, gaps, ()), gaps

    def _namespaces(self, imports: List[Dict[str, Any]], dirs: Tuple[str, ...],
                    gaps: List[Dict[str, Any]], stack: Tuple[str, ...]) -> Namespaces:
        namespaces: Namespaces = {}
        for item in imports:
            # Relative imports need the importing package; notebooks are not
            # packages, so these bind nothing statically here.
            module = item["module"]
            if item["level"] or not module or module in namespaces:
                continue
            located = self._locate(module, dirs, gaps)
            if located is None:
                continue
            kind, path = located
            if kind == "unlisted":
                # The listing failure is already reported as a gap.
                namespaces[module] = None
                continue
            if kind == "ambiguous":
                gap = {"kind": "python_module", "module": module, "paths": list(path),
                       "reason": "module found in more than one searched folder; "
                                 "migration-time runs search ancestor folders "
                                 "outermost first, the saved notebook nearest "
                                 "first, so which file is imported is not known "
                                 "and names from it are not resolved"}
                if gap not in gaps:
                    gaps.append(gap)
                namespaces[module] = None
                continue
            if kind == "namespace":
                # A namespace package defines no names of its own.
                namespaces[module] = ModuleNamespace(values={}, names=frozenset())
                continue
            if kind == "unsupported":
                gap = {"kind": "python_module", "module": module, "path": path,
                       "reason": "Python would load a compiled file, which is not "
                                 "Python source; names from it are not resolved"}
                if gap not in gaps:
                    gaps.append(gap)
                namespaces[module] = None
                continue
            if path in stack or len(stack) >= MAX_IMPORT_DEPTH:
                gaps.append({
                    "kind": "python_module", "module": module, "path": path,
                    "cell_number": item.get("cell_number"),
                    "reason": "import cycle or depth limit; names from this "
                              "import are not resolved",
                })
                namespaces[module] = None
                continue
            namespaces[module] = self._module(module, path, dirs, gaps, stack + (path,))
        return namespaces

    def _module(self, module: str, path: str, dirs: Tuple[str, ...],
                gaps: List[Dict[str, Any]], stack: Tuple[str, ...]) -> Optional[ModuleNamespace]:
        # A module's gaps are cached with its namespace and replayed on every
        # hit, so each importing notebook reports them, not only the first.
        key = (path, dirs)
        if key not in self._modules:
            module_gaps: List[Dict[str, Any]] = []
            namespace = self._analyse_module(module, path, dirs, module_gaps, stack)
            self._modules[key] = (namespace, module_gaps)
        namespace, module_gaps = self._modules[key]
        gaps.extend(gap for gap in module_gaps if gap not in gaps)
        return namespace

    def _analyse_module(self, module: str, path: str, dirs: Tuple[str, ...],
                        gaps: List[Dict[str, Any]], stack: Tuple[str, ...]
                        ) -> Optional[ModuleNamespace]:
        try:
            source = self._read_file(path).decode("utf-8")
        except Exception as exc:  # reported, never silently skipped
            gaps.append({"kind": "python_module", "module": module, "path": path,
                         "reason": f"module could not be read: {exc}"})
            return None
        imported = self._namespaces(
            python_import_froms(_module_notebook(source)), dirs, gaps, stack)
        namespace, errors = module_namespace(source, imported)
        if namespace is not None:
            gaps.extend({**finding, "module": module, "path": path}
                        for finding in namespace.input_gaps)
        for error in errors:
            gaps.append({"kind": "python_module", "module": module, "path": path,
                         "reason": f"module could not be analysed: {error.get('message')}"})
        return namespace

    def _locate(self, module: str, dirs: Tuple[str, ...],
                gaps: List[Dict[str, Any]]) -> Optional[Tuple[str, Any]]:
        """Find ``module`` as CPython's PathFinder would on ``dirs``.

        Returns (kind, path): ``"source"`` with the ``.py`` file Python
        loads, ``"namespace"`` for a namespace package, ``"unsupported"``
        when Python would load a compiled file, ``"ambiguous"`` with every
        candidate when more than one folder holds a module or regular
        package of that name, ``"unlisted"`` when a folder the lookup
        depends on could not be listed (so a hit there cannot be ruled
        out); None when no folder holds it (an installed package or a
        missing module).

        The two notebook bootstraps order the same ancestor folders
        oppositely on ``sys.path`` (``job_migrate``'s migration-time
        walk-up puts the outermost first, the saved notebook's
        ``build_artifact_workspace_bootstrap_cell`` the nearest), so a name
        found in more than one of these folders is not resolved. A
        namespace portion never wins over a module or regular package,
        whatever the order. Folders the migration-time walk-up adds above
        the export root, and its local-module source roots, are not
        searched here.
        """
        search = list(dirs)
        parts = module.split(".")
        for index, part in enumerate(parts):
            portions: List[str] = []
            hits: List[Tuple[str, str]] = []
            for directory in search:
                kind, location = self._find_in(directory, part, gaps)
                if kind == "unlisted":
                    return "unlisted", None
                if kind == "namespace":
                    portions.append(location)
                elif kind is not None:
                    hits.append((kind, location))
            if len(hits) > 1:
                return "ambiguous", [location for _, location in hits]
            found = hits[0] if hits else None
            last = index == len(parts) - 1
            if found is None:
                if not portions:
                    return None
                if last:
                    return "namespace", None
                search = portions
                continue
            kind, location = found
            if kind == "unsupported" or last:
                return kind, location
            if posixpath.basename(location).split(".")[0] != "__init__":
                return None  # a plain module has no submodules
            search = [posixpath.dirname(location)]
        return None

    def _find_in(self, directory: str, part: str, gaps: List[Dict[str, Any]]
                 ) -> Tuple[Optional[str], Optional[str]]:
        """One FileFinder step: a regular package, then a module file (both
        in loader order: extension, source, bytecode), then a namespace
        portion. Returns (kind, path), ("unlisted", None) when a folder
        could not be listed, or (None, None)."""
        listing = self._listing(directory, gaps)
        if directory in self._listing_errors:
            return "unlisted", None
        if listing is None:
            return None, None
        files, folders = listing
        package = posixpath.join(directory, part)
        if part in folders:
            package_listing = self._listing(package, gaps)
            if package in self._listing_errors:
                return "unlisted", None
            init = _loaded_file(package_listing[0] if package_listing else frozenset(), "__init__")
            if init:
                return _kind(init), posixpath.join(package, init)
        module_file = _loaded_file(files, part)
        if module_file:
            return _kind(module_file), posixpath.join(directory, module_file)
        if part in folders:
            return "namespace", package
        return None, None

    def _listing(self, directory: str, gaps: List[Dict[str, Any]]) -> Listing:
        if directory not in self._listings:
            try:
                self._listings[directory] = self._list_entries(directory)
            except Exception as exc:  # reported, never silently skipped
                self._listings[directory] = None
                self._listing_errors[directory] = str(exc)
        if directory in self._listing_errors:
            gap = {"kind": "python_module_search", "directory": directory,
                   "reason": "folder could not be listed; imports from it are "
                             f"not resolved: {self._listing_errors[directory]}"}
            if gap not in gaps:
                gaps.append(gap)
        return self._listings[directory]
