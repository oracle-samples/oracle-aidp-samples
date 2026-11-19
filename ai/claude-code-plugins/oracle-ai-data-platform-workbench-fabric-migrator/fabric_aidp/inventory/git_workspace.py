"""Discover items in a Fabric Git-integration export.

Every item lives in its own directory named `<display name>.<ItemType>`, at the
root or nested. The directory carries either a v2 `.platform` file or the v1 pair
`item.metadata.json` + `item.config.json`.

`metadata.type` is authoritative. A directory suffix can be stale after a rename,
so it is only a fallback for the display name.

A directory that carries one of those files but whose identity cannot be read
out of it is an `UnreadableItem`, not a non-item. It used to be neither: it
was skipped, silently, so a real notebook dropped out of the migration with no
error, no warning and no record, and the operator saw a smaller asset count
and no reason for it. Fabric identifies an item *by* that file, so an
unreadable one is exactly when a human needs telling.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path

PLATFORM_FILE = ".platform"
V1_METADATA_FILE = "item.metadata.json"
V1_CONFIG_FILE = "item.config.json"
_SKIP_DIRS = {".git", ".github", "__pycache__", ".venv", "node_modules"}


class NotAFabricExport(ValueError):
    """The directory does not look like a Fabric Git export."""


@dataclass(frozen=True)
class UnreadableItem:
    """A directory that claims to be a Fabric item but does not say which.

    Deliberately not a `FabricItem`: it has no type, so no scanner can run
    on it and nothing downstream should be able to mistake it for
    something migratable. It exists to be reported.
    """

    path: Path
    reason: str

    @property
    def name(self) -> str:
        """The directory name, which is all the identity there is."""
        return self.path.name


@dataclass(frozen=True)
class FabricItem:
    name: str
    item_type: str
    logical_id: str
    path: Path
    description: str = ""
    # The workspace folder this item sits in, as a posix path relative to the
    # export root, or "" for an item at the root. Fabric's Git integration
    # writes workspace folders as directories, and a display name is unique
    # only inside one of them -- two `Shared_Load.Notebook` directories under
    # `teamA/` and `teamB/` are an ordinary export and used to collide on the
    # asset id `notebook.Shared_Load`, which made `plan` refuse the whole
    # workspace. The folder is what the export already carries to tell them
    # apart, so it is read here and the planner puts it in the id.
    folder: str = ""

    def file(self, *parts) -> Path:
        return self.path.joinpath(*parts)


def _load_json(path: Path):
    """`(object, reason)`; `reason` is set when the file is there but unusable."""
    try:
        text = path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError) as exc:
        return None, f"{path.name} could not be read ({exc})"
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"{path.name} is not valid JSON ({exc})"
    if not isinstance(value, dict):
        return None, f"{path.name} is {type(value).__name__}, not a JSON object"
    return value, ""


def _read_item(directory: Path):
    """A FabricItem, an UnreadableItem, or None when this is not an item."""
    metadata = config = None
    if (directory / PLATFORM_FILE).is_file():
        platform, reason = _load_json(directory / PLATFORM_FILE)
        if reason:
            return UnreadableItem(directory, reason)
        metadata = platform.get("metadata")
        config = platform.get("config")
    elif (directory / V1_METADATA_FILE).is_file():
        metadata, reason = _load_json(directory / V1_METADATA_FILE)
        if reason:
            return UnreadableItem(directory, reason)
        config, _ = _load_json(directory / V1_CONFIG_FILE)
    else:
        # No identity file at all: an ordinary directory on the way to the
        # items, which is the one case that must stay silent.
        return None
    if not isinstance(metadata, dict):
        return UnreadableItem(
            directory, "its metadata block has no `metadata` object")
    item_type = metadata.get("type")
    if not isinstance(item_type, str) or not item_type.strip():
        return UnreadableItem(
            directory, "its metadata records no item type (`metadata.type`)")
    name = metadata.get("displayName")
    if not isinstance(name, str) or not name.strip():
        # Fall back to the directory stem: "Sales.Lakehouse" -> "Sales".
        name = directory.name.rsplit(".", 1)[0] or directory.name
    logical_id = ""
    if isinstance(config, dict) and isinstance(config.get("logicalId"), str):
        logical_id = config["logicalId"]
    description = metadata.get("description")
    return FabricItem(
        name=name,
        item_type=item_type.strip(),
        logical_id=logical_id,
        path=directory,
        description=description if isinstance(description, str) else "",
    )


def _folder_of(directory: Path, root: Path) -> str:
    """The workspace folder holding `directory`, posix, relative to `root`.

    "" for an item directly under the root, which is where the three
    bundled estates and all 78 staged real artifacts sit -- so the common
    case keeps the short id it already had.
    """
    try:
        relative = directory.resolve().relative_to(root.resolve())
    except (ValueError, OSError):
        return ""
    parent = relative.parent.as_posix()
    return "" if parent in (".", "") else parent


def discover_items(root, problems=None) -> list:
    """Walk `root` and return every Fabric item found, deterministically ordered.

    `problems` is an optional list; every directory that claims to be an
    item but does not say which gets an `UnreadableItem` appended to it, in
    path order. Pass one unless you have somewhere else to report them:
    without it they are still skipped, and that is the defect.
    """
    root = Path(root)
    if not root.is_dir():
        raise NotAFabricExport(f"not a directory: {root}")

    found, unreadable = [], []
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except OSError:
            continue
        item = _read_item(current)
        if isinstance(item, UnreadableItem):
            # Still an item directory as far as Fabric is concerned, so its
            # contents are its definition and not more items.
            unreadable.append(item)
            continue
        if item is not None:
            found.append(replace(item, folder=_folder_of(current, root)))
            continue
        for entry in entries:
            if entry.is_dir() and not entry.is_symlink() and entry.name not in _SKIP_DIRS:
                stack.append(entry)

    unreadable.sort(key=lambda u: u.path.as_posix())
    if problems is not None:
        problems.extend(unreadable)

    if not found:
        if unreadable:
            raise NotAFabricExport(
                f"{len(unreadable)} director(ies) under {root} carry a "
                f"{PLATFORM_FILE} file but none of them says what it is, so "
                f"there is nothing to scan: "
                + "; ".join(f"{u.path.name} — {u.reason}" for u in unreadable))
        raise NotAFabricExport(
            f"no Fabric items under {root} — expected directories containing a "
            f"{PLATFORM_FILE} file. Point this at the folder your workspace is "
            f"synced to, not at the repository root if items live in a subfolder."
        )
    # The folder is part of the key because it is part of the identity: two
    # items of one type can share a display name in two folders, and without
    # it their order would depend on the filesystem walk.
    return sorted(found, key=lambda i: (i.item_type, i.name, i.folder))


def items_of_type(items, item_type: str) -> list:
    wanted = item_type.casefold()
    return [i for i in items if i.item_type.casefold() == wanted]
