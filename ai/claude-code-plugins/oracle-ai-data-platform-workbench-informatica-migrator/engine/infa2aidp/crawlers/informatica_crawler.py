"""Informatica PowerCenter 10.x Repository Crawler.

Connects to Informatica PowerCenter via SOAP Web Services Hub (MetadataService)
or PMREP CLI to programmatically browse repository metadata and export mappings,
workflows, and sessions as XML — eliminating manual export.

Supported access methods (tried in order):
  1. SOAP MetadataService — http://<host>:<port>/wsh/services/MetadataService
  2. PMREP CLI — requires pmrep installed on the network

NOTE: PowerCenter 10.x on-premises does NOT have a REST API for repository
operations. REST endpoints (icSessionId) belong to Informatica Intelligent
Cloud Services (IICS) — a separate product. For on-prem PowerCenter, use
SOAP or PMREP.

NOTE: The SOAP Web Services Hub cannot export objects as XML. For XML export,
the crawler always falls back to pmrep objectexport. SOAP is used only for
browsing metadata (listing folders, mappings, workflows).

Target environment: PowerCenter 10.5 repositories (current on-prem Informatica).

References:
  - Informatica PowerCenter 10.5 Web Services Guide
  - Informatica PowerCenter 10.5 Command Reference (pmrep)
  - Informatica PowerCenter 10.5 Administrator Guide
"""

import json
import logging
import os
import subprocess
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# SOAP namespace for Informatica Web Services Hub
_WSH_NS = "http://www.informatica.com/wsh"


@dataclass
class InfaConnectionConfig:
    """Informatica PowerCenter connection configuration."""
    host: str
    port: int = 7333                   # WSH default HTTP port (7343 for HTTPS)
    username: str = ""
    password: str = ""
    domain: str = ""                    # Informatica domain name
    repository: str = ""                # Repository service name
    security_domain: str = "Native"
    # SOAP Web Services Hub URL (auto-built from host:port if empty)
    wsh_url: str = ""
    # PMREP / PMCMD paths
    pmrep_path: str = "pmrep"
    pmcmd_path: str = "pmcmd"
    # Integration service name (for pmcmd workflow execution)
    integration_service: str = ""
    # TLS verification for the Web Services Hub: True, False, or the path
    # of a CA bundle (PEM) for a corporate CA. Default on.
    verify_tls: "bool | str" = True


@dataclass
class InfaObject:
    """Metadata for an Informatica repository object."""
    name: str
    folder: str = ""
    object_type: str = ""    # mapping, workflow, session, source, target
    description: str = ""
    last_modified: str = ""
    version: int = 0


@dataclass
class InfaCrawlResult:
    """Result of crawling an Informatica repository."""
    folders: list[str] = field(default_factory=list)
    mappings: list[InfaObject] = field(default_factory=list)
    workflows: list[InfaObject] = field(default_factory=list)
    sessions: list[InfaObject] = field(default_factory=list)
    connections: list[dict] = field(default_factory=list)
    exported_xml_files: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class InformaticaCrawler:
    """Crawls Informatica PowerCenter 10.x repository to extract ETL metadata.

    Supports two access methods (auto-detected or configurable):
    - SOAP Web Services Hub (MetadataService) — for browsing metadata
    - PMREP CLI — for browsing AND exporting objects as XML

    XML export always uses pmrep objectexport (SOAP WSH cannot export XML).
    """

    def __init__(self, config: InfaConnectionConfig):
        self.config = config
        self._session_id: Optional[str] = None
        self._access_method: Optional[str] = None
        self._pmrep_connected: bool = False
        self._http = requests.Session()
        # Verification used to be hard-coded off "because many on-prem installs
        # use self-signed certs" -- which sent the repository password over an
        # unverified channel to every host. On by default; a corporate CA is a
        # bundle path, and False is an explicit opt-out for a lab host.
        self._http.verify = config.verify_tls

    # ------------------------------------------------------------------
    # Connection & auto-detect
    # ------------------------------------------------------------------

    def connect(self, method: str = "auto") -> str:
        """Connect to Informatica repository.

        Args:
            method: "soap", "pmrep", or "auto" (try soap then pmrep).

        Returns:
            The access method that succeeded.

        Raises:
            ConnectionError: If no method works.
        """
        methods = (
            [method] if method != "auto"
            else ["soap", "pmrep"]
        )

        last_error = None
        for m in methods:
            try:
                if m == "soap":
                    self._connect_soap()
                elif m == "pmrep":
                    self._connect_pmrep()
                else:
                    raise ValueError(f"Unknown method: {m}")
                self._access_method = m
                logger.info("Connected via %s", m)
                return m
            except Exception as exc:
                last_error = exc
                logger.debug("Method %s failed: %s", m, exc)

        raise ConnectionError(
            f"All connection methods failed. Last error: {last_error}"
        )

    def disconnect(self):
        """Close the repository connection."""
        try:
            if self._access_method == "soap" and self._session_id:
                self._soap_call("MetadataService", "Logout", "")
            if self._pmrep_connected:
                self._pmrep_run("close")
        except Exception:
            pass
        self._session_id = None
        self._pmrep_connected = False
        self._access_method = None

    # ------------------------------------------------------------------
    # SOAP Web Services Hub (MetadataService)
    # ------------------------------------------------------------------

    def _connect_soap(self):
        """Connect via SOAP MetadataService LoginRequest."""
        wsh = self.config.wsh_url
        if not wsh:
            proto = "https" if self.config.port == 7343 else "http"
            wsh = f"{proto}://{self.config.host}:{self.config.port}/wsh/services"
            self.config.wsh_url = wsh

        # Login via MetadataService
        body_xml = f"""
            <LoginRequest>
                <RepositoryDomainName>{self.config.domain}</RepositoryDomainName>
                <RepositoryName>{self.config.repository}</RepositoryName>
                <UserName>{self.config.username}</UserName>
                <Password>{self.config.password}</Password>
            </LoginRequest>"""

        resp_xml = self._soap_raw_call("MetadataService", "Login", body_xml)
        root = ET.fromstring(resp_xml)

        # Extract SessionId from response
        session_el = root.find(".//{*}SessionId")
        if session_el is None:
            raise ConnectionError("SOAP login returned no SessionId")
        self._session_id = session_el.text
        logger.info("SOAP login successful, session: %s...", self._session_id[:16])

    def _soap_raw_call(self, service: str, operation: str, body_xml: str) -> str:
        """Execute a raw SOAP call against a specific WSH service."""
        # Build Context header with session ID if authenticated
        context_xml = ""
        if self._session_id:
            context_xml = f"""
        <ns:Context xmlns:ns="{_WSH_NS}">
            <SessionId>{self._session_id}</SessionId>
        </ns:Context>"""

        envelope = f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                  xmlns:wsh="{_WSH_NS}">
    <soapenv:Header>{context_xml}
    </soapenv:Header>
    <soapenv:Body>
        <wsh:{operation}>{body_xml}
        </wsh:{operation}>
    </soapenv:Body>
</soapenv:Envelope>"""

        url = f"{self.config.wsh_url}/{service}"
        resp = self._http.post(
            url,
            data=envelope.encode("utf-8"),
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "SOAPAction": operation,
            },
            timeout=120,
        )
        resp.raise_for_status()
        return resp.text

    def _soap_call(self, service: str, operation: str, body_xml: str) -> str:
        """Execute an authenticated SOAP call."""
        if not self._session_id:
            raise ConnectionError("Not authenticated. Call connect() first.")
        return self._soap_raw_call(service, operation, body_xml)

    def _list_folders_soap(self) -> list[str]:
        """List all repository folders via MetadataService.getAllFolders."""
        resp_xml = self._soap_call("MetadataService", "getAllFolders", "")
        root = ET.fromstring(resp_xml)
        folders = []
        # getAllFolders returns FolderInfo elements with Name attribute or child
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag in ("FolderName", "Name") and el.text:
                name = el.text.strip()
                if name and name not in folders:
                    folders.append(name)
        return folders

    def _list_objects_soap(self, folder: str, obj_type: str) -> list[InfaObject]:
        """List objects in a folder via MetadataService.

        Uses type-specific operations: getAllMappings, getAllWorkflows, getAllSessions.
        """
        # Map our type names to SOAP operation names
        operation_map = {
            "mapping": "getAllMappings",
            "workflow": "getAllWorkflows",
            "session": "getAllSessions",
            "source": "getAllSourceDefinitions",
            "target": "getAllTargetDefinitions",
        }
        operation = operation_map.get(obj_type)
        if not operation:
            logger.warning("No SOAP operation for type: %s", obj_type)
            return []

        body_xml = f"<FolderName>{folder}</FolderName>"
        objects = []
        try:
            resp_xml = self._soap_call("MetadataService", operation, body_xml)
            root = ET.fromstring(resp_xml)

            # Extract object names from response
            for el in root.iter():
                tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if tag in ("MappingInfo", "WorkflowInfo", "SessionInfo",
                           "SourceInfo", "TargetInfo"):
                    # Object element — look for Name attribute or child
                    name = el.get("Name") or el.findtext("{*}Name") or ""
                    desc = el.get("Description") or el.findtext("{*}Description") or ""
                    if name:
                        objects.append(InfaObject(
                            name=name, folder=folder,
                            object_type=obj_type, description=desc,
                        ))
                # (A stdlib ElementTree Element has no getparent(); the
                # former "elif tag == 'Name' and el.getparent is None"
                # branch raised AttributeError on the first <Name> element,
                # which the handler below swallowed -- so every folder
                # listed at most one object and logged a warning.)

        except Exception as exc:
            logger.warning("SOAP %s in %s failed: %s", operation, folder, exc)

        return objects

    # ------------------------------------------------------------------
    # PMREP CLI (PowerCenter Command Line)
    # ------------------------------------------------------------------

    def _connect_pmrep(self):
        """Connect to repository via pmrep connect."""
        cmd = [
            self.config.pmrep_path, "connect",
            "-r", self.config.repository,
            "-d", self.config.domain,
            "-n", self.config.username,
            # -X names an environment variable; -x put the password on the
            # command line, where every user on the host can read it in ps.
            "-X", "INFA_PMREP_PASSWORD",
        ]
        if self.config.security_domain:
            cmd.extend(["-s", self.config.security_domain])

        result = self._pmrep_run_raw(cmd, env={"INFA_PMREP_PASSWORD": self.config.password})
        if "connect completed successfully" not in result.lower():
            raise ConnectionError(f"PMREP connect failed: {result}")
        self._pmrep_connected = True

    def _ensure_pmrep(self):
        """Ensure pmrep is connected (needed for XML export even in SOAP mode)."""
        if not self._pmrep_connected:
            self._connect_pmrep()

    def _pmrep_run(self, *args) -> str:
        cmd = [self.config.pmrep_path] + list(args)
        return self._pmrep_run_raw(cmd)

    @staticmethod
    def _pmrep_run_raw(cmd: list, env: Optional[dict] = None) -> str:
        try:
            run_env = {**os.environ, **env} if env else None
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300, env=run_env
            )
            return result.stdout + result.stderr
        except FileNotFoundError:
            raise ConnectionError(
                "pmrep binary not found on PATH. "
                "Ensure Informatica client tools are installed."
            )
        except subprocess.TimeoutExpired:
            raise ConnectionError("pmrep command timed out (300s)")

    def _parse_pmrep_output(self, output: str, skip_prefixes: tuple = None) -> list[str]:
        """Parse pmrep output lines, skipping headers/footers."""
        if skip_prefixes is None:
            skip_prefixes = (".", "Informatica", "Copyright", "connect ",
                             "listobjects ", "listfolders ", "listconnections ",
                             "objectexport ", "completed successfully")
        lines = []
        for line in output.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            if any(line.lower().startswith(p.lower()) for p in skip_prefixes):
                continue
            if "completed successfully" in line.lower():
                continue
            lines.append(line)
        return lines

    def _list_folders_pmrep(self) -> list[str]:
        """List all repository folders via pmrep listfolders."""
        output = self._pmrep_run("listfolders")
        return self._parse_pmrep_output(output)

    def _list_objects_pmrep(self, folder: str, obj_type: str) -> list[InfaObject]:
        """List objects via pmrep listobjects.

        Object types are lowercase: mapping, workflow, session, source, target,
        transformation, mapplet, worklet, task, scheduler, sessionconfig.
        """
        objects = []
        try:
            output = self._pmrep_run(
                "listobjects", "-o", obj_type, "-f", folder
            )
            for line in self._parse_pmrep_output(output):
                # Output format: <object_type>  <object_name>
                parts = line.split(None, 1)
                if len(parts) >= 2:
                    name = parts[1].strip()
                elif len(parts) == 1:
                    name = parts[0].strip()
                else:
                    continue
                objects.append(InfaObject(
                    name=name, folder=folder, object_type=obj_type
                ))
        except Exception as exc:
            logger.warning("PMREP list %s in %s failed: %s", obj_type, folder, exc)
        return objects

    def _export_xml_pmrep(self, folder: str, obj_name: str, obj_type: str,
                          output_dir: str) -> Optional[str]:
        """Export an object as PowerCenter XML via pmrep objectexport.

        This is the ONLY way to get XML exports programmatically.
        SOAP WSH does not support XML export.
        """
        safe_name = obj_name.replace("/", "_").replace("\\", "_")
        path = os.path.join(output_dir, f"{safe_name}.xml")
        try:
            output = self._pmrep_run(
                "objectexport",
                "-o", obj_type,
                "-n", obj_name,
                "-f", folder,
                "-u", path,
                "-b",   # include dependent objects (sources, targets, etc.)
            )
            if os.path.isfile(path) and os.path.getsize(path) > 0:
                return path
            logger.warning("PMREP export produced empty file for %s: %s",
                           obj_name, output)
        except Exception as exc:
            logger.warning("PMREP export %s/%s failed: %s", folder, obj_name, exc)
        return None

    def _list_connections_pmrep(self) -> list[dict]:
        """List connection objects via pmrep listconnections."""
        connections = []
        try:
            output = self._pmrep_run("listconnections")
            for line in self._parse_pmrep_output(output):
                # Output: <connection_name> <connection_type> <connection_subtype>
                parts = line.split(None, 2)
                conn = {"name": parts[0] if parts else line}
                if len(parts) >= 2:
                    conn["type"] = parts[1]
                if len(parts) >= 3:
                    conn["subtype"] = parts[2]
                connections.append(conn)
        except Exception as exc:
            logger.warning("PMREP listconnections failed: %s", exc)
        return connections

    # ------------------------------------------------------------------
    # Unified API (dispatches to active method)
    # ------------------------------------------------------------------

    def list_folders(self) -> list[str]:
        """List all repository folders."""
        if self._access_method == "soap":
            return self._list_folders_soap()
        elif self._access_method == "pmrep":
            return self._list_folders_pmrep()
        return []

    def list_mappings(self, folder: str) -> list[InfaObject]:
        """List all mappings in a folder."""
        return self._list_objects("mapping", folder)

    def list_workflows(self, folder: str) -> list[InfaObject]:
        """List all workflows in a folder."""
        return self._list_objects("workflow", folder)

    def list_sessions(self, folder: str) -> list[InfaObject]:
        """List all sessions in a folder."""
        return self._list_objects("session", folder)

    def list_connections(self) -> list[dict]:
        """List all connection objects (always via pmrep)."""
        if self._access_method == "pmrep" or self._pmrep_connected:
            return self._list_connections_pmrep()
        # Try connecting pmrep for connections even in soap mode
        try:
            self._ensure_pmrep()
            return self._list_connections_pmrep()
        except Exception:
            return []

    def _list_objects(self, obj_type: str, folder: str) -> list[InfaObject]:
        if self._access_method == "soap":
            return self._list_objects_soap(folder, obj_type)
        elif self._access_method == "pmrep":
            return self._list_objects_pmrep(folder, obj_type)
        return []

    def export_object_xml(self, folder: str, obj_name: str, obj_type: str,
                          output_dir: str) -> Optional[str]:
        """Export a single object as PowerCenter XML.

        Always uses pmrep objectexport — the only way to get XML exports.
        If primary connection is SOAP, pmrep is auto-connected for export.
        """
        os.makedirs(output_dir, exist_ok=True)
        try:
            self._ensure_pmrep()
        except Exception as exc:
            logger.error(
                "pmrep required for XML export but unavailable: %s", exc
            )
            return None
        return self._export_xml_pmrep(folder, obj_name, obj_type, output_dir)

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def crawl_repository(self, output_dir: str,
                         folders: Optional[list[str]] = None,
                         export_xml: bool = True) -> InfaCrawlResult:
        """Crawl the entire repository (or specified folders).

        Args:
            output_dir: Directory to store exported XML files.
            folders: Optional list of folders to crawl (None = all).
            export_xml: Whether to export XML files (True) or just list metadata.

        Returns:
            InfaCrawlResult with all discovered objects and exported files.
        """
        result = InfaCrawlResult()

        # Discover folders
        if folders:
            result.folders = folders
        else:
            try:
                result.folders = self.list_folders()
            except Exception as exc:
                result.errors.append(f"Failed to list folders: {exc}")
                return result

        logger.info("Crawling %d folders", len(result.folders))

        for folder in result.folders:
            logger.info("Crawling folder: %s", folder)

            # List mappings
            mappings = self.list_mappings(folder)
            result.mappings.extend(mappings)
            logger.info("  %d mappings found", len(mappings))

            # List workflows
            workflows = self.list_workflows(folder)
            result.workflows.extend(workflows)
            logger.info("  %d workflows found", len(workflows))

            # List sessions
            sessions = self.list_sessions(folder)
            result.sessions.extend(sessions)
            logger.info("  %d sessions found", len(sessions))

            # Export XMLs via pmrep objectexport
            if export_xml:
                folder_dir = os.path.join(output_dir, folder.replace("/", "_"))
                os.makedirs(folder_dir, exist_ok=True)

                # Export workflows first (includes dependent mappings/sessions)
                for wf in workflows:
                    xml_path = self.export_object_xml(
                        folder, wf.name, "workflow", folder_dir
                    )
                    if xml_path:
                        result.exported_xml_files.append(xml_path)
                        logger.info("    Exported: %s", os.path.basename(xml_path))
                    else:
                        result.errors.append(
                            f"Failed to export workflow {folder}/{wf.name}"
                        )

                # Export standalone mappings not already in workflow exports
                wf_mapping_names = set()
                for s in sessions:
                    wf_mapping_names.add(s.name)

                for m in mappings:
                    if m.name not in wf_mapping_names:
                        xml_path = self.export_object_xml(
                            folder, m.name, "mapping", folder_dir
                        )
                        if xml_path:
                            result.exported_xml_files.append(xml_path)

        # List connection objects
        try:
            result.connections = self.list_connections()
        except Exception as exc:
            result.errors.append(f"Failed to list connections: {exc}")

        logger.info(
            "Crawl complete: %d mappings, %d workflows, %d XMLs exported, %d errors",
            len(result.mappings), len(result.workflows),
            len(result.exported_xml_files), len(result.errors),
        )
        return result

    def crawl_and_migrate(self, output_dir: str,
                          folders: Optional[list[str]] = None) -> InfaCrawlResult:
        """Crawl repository and prepare XML files for infa2aidp migrate."""
        xml_dir = os.path.join(output_dir, "exported_xml")
        return self.crawl_repository(xml_dir, folders=folders, export_xml=True)

    # ------------------------------------------------------------------
    # Inventory report
    # ------------------------------------------------------------------

    def generate_inventory_report(self, result: InfaCrawlResult,
                                  output_path: str) -> str:
        """Generate a markdown inventory report from crawl results."""
        lines = [
            "# Informatica Repository Inventory",
            "",
            f"**Repository:** {self.config.repository}",
            f"**Host:** {self.config.host}",
            f"**Access Method:** {self._access_method}",
            f"**Folders:** {len(result.folders)}",
            "",
            "## Summary",
            "",
            "| Metric | Count |",
            "|--------|-------|",
            f"| Folders | {len(result.folders)} |",
            f"| Mappings | {len(result.mappings)} |",
            f"| Workflows | {len(result.workflows)} |",
            f"| Sessions | {len(result.sessions)} |",
            f"| Connections | {len(result.connections)} |",
            f"| Exported XMLs | {len(result.exported_xml_files)} |",
            f"| Errors | {len(result.errors)} |",
            "",
        ]

        # Folder breakdown
        lines.extend(["## Folder Breakdown", ""])
        lines.append("| Folder | Mappings | Workflows | Sessions |")
        lines.append("|--------|----------|-----------|----------|")
        for folder in result.folders:
            m_count = sum(1 for m in result.mappings if m.folder == folder)
            w_count = sum(1 for w in result.workflows if w.folder == folder)
            s_count = sum(1 for s in result.sessions if s.folder == folder)
            lines.append(f"| {folder} | {m_count} | {w_count} | {s_count} |")
        lines.append("")

        if result.errors:
            lines.extend(["## Errors", ""])
            for err in result.errors:
                lines.append(f"- {err}")
            lines.append("")

        report = "\n".join(lines)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        return output_path
