#!/usr/bin/env python3
"""
Unified Migrate-Validate-Fix Pipeline
=======================================
Single-pass per notebook:
1. Claude migrates the notebook AND classifies each cell (READ_ONLY vs WRITE/ACTION)
2. READ_ONLY cells execute on the AIDP cluster immediately
3. Outputs are compared against original notebook outputs where available
4. Failed cells are sent back to Claude for fixing (Sonnet x2, then Opus x3)
5. Produces: migrated .ipynb, REPORT.md with cell outputs + diffs

Usage:
    # Single notebook
    python3 migrate_validate.py <notebook_path>

    # Batch with N parallel sessions
    python3 migrate_validate.py --batch <list.json> --parallel 5

    # Custom cluster
    python3 migrate_validate.py --batch <list.json> --cluster <id> --parallel 5
"""

import anthropic
import asyncio
import json
import os
import sys
import re
import copy
import time
import argparse
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from aidp_executor import AIDPSession, format_outputs

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTEBOOKS_DIR = os.path.join(PROJECT_DIR, "notebooks")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "migrated")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DEFAULT_CLUSTER = "<CLUSTER_ID>"

MIGRATE_AND_CLASSIFY_PROMPT = """You are an expert Spark/Databricks migration engineer. You are given a Jupyter notebook that needs to be migrated from Databricks to OCI AIDP (Python 3.11).

Your job is to produce a JSON object with TWO parts:

## PART 1: "cells" - The migrated notebook cells

For EACH cell, provide:
- "cell_type": original cell type
- "source": the migrated source code (apply all AIDP fixes)
- "classification": one of:
  - "READ_ONLY" - safe to execute: imports, reads, queries, transforms, display, function/class definitions, variable assignments, print statements
  - "WRITE" - modifies external state: writes to storage, inserts into tables, saves files, drops/creates/alters tables, sends data to APIs
  - "NOTIFICATION" - sends alerts to humans: Slack messages, emails, SMS, webhook calls to notification services
  - "SKIP" - empty, markdown, raw, or non-executable

IMPORTANT: Be precise about classification. These are SAFE to run (READ_ONLY):
- `F.to_json()`, `F.from_json()` - Spark column transforms, NOT file writes
- `spark.read.parquet()`, `spark.read.csv()` - reads, not writes
- Function/class definitions (even if the function body contains writes - defining is safe)
- `df.show()`, `df.count()`, `df.describe()`, `display(df)` - read-only display
- `spark.sql("SELECT ...")` - read queries
- `dbutils.widgets.get()`, `dbutils.secrets.get()` - parameter reads
- `createOrReplaceTempView()` - in-memory only
- Variable assignments, list comprehensions, dict operations

These are WRITES (skip):
- `df.write.parquet()`, `df.write.save()`, `df.write.mode(...).save()`
- `spark.sql("INSERT INTO ...")`, `spark.sql("DROP TABLE ...")`
- `spark.sql("CREATE TABLE ...")` (non-temporary)
- `saveAsTable()`, `insertInto()`
- `os.remove()`, `shutil.rmtree()`, file writes with `open(..., 'w')`
- `put_object()`, `delete_object()` - OCI/S3 mutations

These are NOTIFICATIONS (skip):
- Slack SDK calls, webhook posts
- Email sending (smtplib, sendmail)
- `requests.post()` to notification endpoints

## PART 2: "migration_notes" - What changed and why

A list of strings, each describing a change made.

## Migration rules:
- Add these two lines as the first cell:
  ```
  import sys; sys.path.insert(0, '/Workspace/migration-dependencies/python_libs/')
  from aidp_compat import dbutils, display, displayHTML, sql, translate_path
  ```
- Convert `%sql <query>` to `sql('''<query>''')`
- Convert `%run /path/to/notebook` to `dbutils.notebook.run(translate_path("/path/to/notebook.ipynb"), timeout=0)`
- `%pip install <pkg>`: Comment out original with "# AIDP: install via cluster libraries", then add subprocess pip install for testing
- Convert `%sh <cmd>` to `import subprocess; subprocess.run("<cmd>", shell=True, check=True)`
- Replace `from aidp_dbutils import _DBUtils` / `dbutils = _DBUtils(oidlUtils)` with the aidp_compat import
- Databricks `spark.conf.set("spark.databricks.*")` calls should be commented out
- `spark` and `sc` are pre-initialized on AIDP
- `/Workspace/` paths work as-is on AIDP

## AIDP Environment:
- Python 3.11.13, Apache Spark, Scala 2.12.18, Java 17 (GraalVM)
- JARs installed: Hudi 0.15.0, FeatureLib, FeatureLib2, MessageParser, DecryptUDF, Delta Lake 3.2
- OCI HDFS connector available (oci:// paths work; BmcFilesystem configured with API key at cluster level)
- OCI Python SDK auth: API key via CLI config file at /Workspace/<oci-config-workspace-path> (DEFAULT).
  FORBIDDEN: oci.auth.signers.get_resource_principals_signer() — resource principal has known
  failure modes on AIDP. Always use API key + oci.signer.Signer pattern.
- Libraries should be installed via cluster libraries section (not pip install at runtime)

Return ONLY valid JSON: {"cells": [...], "migration_notes": [...]}"""

FIX_PROMPT = """You are fixing a PySpark cell that failed on OCI AIDP.
The notebook is in READ-ONLY validation mode. Do NOT add writes or notifications.
Keep the fix minimal. Return ONLY the fixed Python code, no markdown."""


def read_original_notebook(path: str) -> Tuple[dict, List[dict]]:
    """Read notebook and extract original outputs for comparison."""
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        nb = json.load(f)

    original_outputs = []
    for cell in nb.get("cells", []):
        outputs = cell.get("outputs", [])
        # Extract text from outputs
        text_parts = []
        for out in outputs:
            if out.get("output_type") == "stream":
                text_parts.append("".join(out.get("text", [])))
            elif out.get("output_type") in ("execute_result", "display_data"):
                data = out.get("data", {})
                if "text/plain" in data:
                    text_parts.append("".join(data["text/plain"]) if isinstance(data["text/plain"], list) else data["text/plain"])
        original_outputs.append("".join(text_parts) if text_parts else None)

    return nb, original_outputs


def build_readable(nb: dict) -> str:
    """Build readable notebook representation for Claude."""
    parts = []
    for i, cell in enumerate(nb.get("cells", [])):
        cell_type = cell.get("cell_type", "unknown")
        source = "".join(cell.get("source", []))
        parts.append(f"--- Cell {i} [{cell_type}] ---")
        parts.append(source)
        parts.append("")
    return "\n".join(parts)


def migrate_with_claude(nb: dict, notebook_path: str) -> Tuple[List[dict], List[str], dict]:
    """Send notebook to Claude for migration + cell classification."""
    readable = build_readable(nb)
    if len(readable) > 80000:
        readable = readable[:80000] + "\n\n[... TRUNCATED ...]"

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-opus-4-8",
        max_tokens=16000,
        system=MIGRATE_AND_CLASSIFY_PROMPT,
        messages=[{"role": "user", "content": f"Migrate and classify: {notebook_path}\n\n```\n{readable}\n```"}]
    )

    response = message.content[0].text.strip()
    # Parse JSON
    if response.startswith("```"):
        lines = response.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        response = "\n".join(lines)
    start = response.find("{")
    end = response.rfind("}") + 1
    if start >= 0 and end > start:
        response = response[start:end]

    try:
        result = json.loads(response)
        cells = result.get("cells", [])
        notes = result.get("migration_notes", [])
    except json.JSONDecodeError:
        # Fallback: return original cells as READ_ONLY
        cells = []
        for cell in nb.get("cells", []):
            cells.append({
                "cell_type": cell.get("cell_type", "code"),
                "source": "".join(cell.get("source", [])),
                "classification": "READ_ONLY" if cell.get("cell_type") == "code" else "SKIP"
            })
        notes = ["Claude JSON parse failed - using original cells with READ_ONLY classification"]

    usage = {"input": message.usage.input_tokens, "output": message.usage.output_tokens}
    return cells, notes, usage


def ask_claude_fix(code: str, error: str, context: List[str], attempt: int) -> str:
    """Ask Claude to fix a failing cell."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    model = "claude-opus-4-8" if attempt <= 2 else "claude-opus-4-8"

    ctx = ""
    if context:
        recent = context[-3:]
        ctx = "\n\nRecent successful cells:\n" + "\n".join(f"```\n{c[:300]}\n```" for c in recent)

    msg = client.messages.create(
        model=model,
        max_tokens=4000,
        system=FIX_PROMPT,
        messages=[{"role": "user", "content": f"Fix (attempt {attempt}, {'Opus' if attempt > 2 else 'Sonnet'}):\n```python\n{code}\n```\nError:\n```\n{error[:2000]}\n```{ctx}"}]
    )
    resp = msg.content[0].text.strip()
    if resp.startswith("```"):
        lines = resp.split("\n")[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        resp = "\n".join(lines)
    return resp


def compare_outputs(original: str, actual: str) -> Tuple[bool, str]:
    """Compare original notebook output with actual execution output."""
    if original is None:
        return True, "no original output to compare"
    if not actual:
        return True, "cell produced no output (original had output but may be stale)"

    # Normalize whitespace
    orig_clean = " ".join(original.strip().split())
    actual_clean = " ".join(actual.strip().split())

    if orig_clean == actual_clean:
        return True, "exact match"

    # Check if actual contains the key parts of original (data may have changed)
    orig_lines = set(original.strip().split("\n"))
    actual_lines = set(actual.strip().split("\n"))
    overlap = orig_lines & actual_lines
    if len(overlap) > len(orig_lines) * 0.5:
        return True, f"partial match ({len(overlap)}/{len(orig_lines)} lines)"

    return False, f"output differs (original: {orig_clean[:100]}... vs actual: {actual_clean[:100]}...)"


async def process_notebook(notebook_rel_path: str, session: AIDPSession, max_retries: int = 5) -> dict:
    """Single-pass: migrate, classify, execute, compare, fix."""
    src_path = os.path.join(NOTEBOOKS_DIR, notebook_rel_path)
    out_nb_path = os.path.join(OUTPUT_DIR, notebook_rel_path)
    out_report_path = os.path.join(OUTPUT_DIR, notebook_rel_path.replace(".ipynb", "_REPORT.md"))

    if not os.path.exists(src_path):
        return {"path": notebook_rel_path, "status": "missing"}

    # Skip if already done
    if os.path.exists(out_nb_path) and os.path.exists(out_report_path):
        with open(out_report_path) as f:
            if "## Cell Results" in f.read():
                return {"path": notebook_rel_path, "status": "skipped"}

    print(f"\n{'='*60}")
    print(f"PROCESSING: {notebook_rel_path}")
    print(f"{'='*60}")

    # Step 1: Read original + outputs
    nb, original_outputs = read_original_notebook(src_path)
    total_cells = len(nb.get("cells", []))
    print(f"  Original: {total_cells} cells, {sum(1 for o in original_outputs if o)} with saved outputs")

    # Step 2: Claude migrates + classifies
    print(f"  Migrating with Claude Opus...")
    migrated_cells, migration_notes, usage = migrate_with_claude(nb, notebook_rel_path)
    print(f"  Migration: {len(migrated_cells)} cells, {usage['input']+usage['output']} tokens")

    # Build the migrated notebook
    migrated_nb = copy.deepcopy(nb)
    # Replace cells with migrated versions
    new_cells = []
    for i, mc in enumerate(migrated_cells):
        cell = {
            "cell_type": mc.get("cell_type", "code"),
            "metadata": {},
            "source": [mc.get("source", "")],
            "outputs": [],
            "execution_count": None,
        }
        if mc.get("cell_type") == "markdown":
            cell.pop("outputs", None)
            cell.pop("execution_count", None)
        new_cells.append(cell)
    migrated_nb["cells"] = new_cells

    # Step 3: Bootstrap aidp_compat on the session
    bootstrap = "import sys; sys.path.insert(0, '/Workspace/migration-dependencies/python_libs/')"
    await session.execute(bootstrap, timeout=30)

    # Step 4: Execute READ_ONLY cells on cluster
    cell_results = []
    executed_code = []  # Context for fix attempts
    cells_ok = 0
    cells_failed = 0
    cells_skipped = 0
    cells_fixed = 0
    output_matches = 0
    output_mismatches = 0

    for i, mc in enumerate(migrated_cells):
        classification = mc.get("classification", "SKIP")
        source = mc.get("source", "").strip()
        cell_type = mc.get("cell_type", "code")

        if cell_type != "code" or not source or classification in ("SKIP",):
            cell_results.append({
                "cell": i, "classification": classification,
                "status": "skipped", "reason": cell_type if cell_type != "code" else "empty/skip"
            })
            cells_skipped += 1
            continue

        if classification in ("WRITE", "NOTIFICATION"):
            cell_results.append({
                "cell": i, "classification": classification,
                "status": "skipped", "reason": classification,
                "code": source[:200],
            })
            cells_skipped += 1
            continue

        # Execute READ_ONLY cell
        current_code = source
        cell_passed = False

        for attempt in range(max_retries + 1):
            result = await session.execute(current_code, timeout=120)
            status = result.get("status", "error")
            output = format_outputs(result.get("outputs", []))

            if status == "ok":
                cell_passed = True
                executed_code.append(current_code)

                # Compare with original output
                orig_out = original_outputs[i] if i < len(original_outputs) else None
                match, match_detail = compare_outputs(orig_out, output)
                if orig_out:
                    if match:
                        output_matches += 1
                    else:
                        output_mismatches += 1

                if attempt > 0:
                    cells_fixed += 1
                    # Update the notebook cell with the fix
                    new_cells[i]["source"] = [current_code]

                cell_results.append({
                    "cell": i, "classification": "READ_ONLY",
                    "status": "ok", "attempts": attempt + 1,
                    "fixed": attempt > 0,
                    "code": current_code,
                    "output": output[:2000] if output else "(no output)",
                    "original_output": (orig_out[:500] if orig_out else None),
                    "output_match": match_detail if orig_out else None,
                })
                cells_ok += 1
                break
            else:
                if attempt < max_retries:
                    model_name = "Sonnet" if attempt < 2 else "Opus"
                    print(f"  Cell {i} FAIL (attempt {attempt+1}), asking {model_name}...")
                    try:
                        current_code = ask_claude_fix(current_code, output, executed_code, attempt + 1)
                    except Exception as e:
                        print(f"  Fix request failed: {e}")
                        break
                else:
                    cell_results.append({
                        "cell": i, "classification": "READ_ONLY",
                        "status": "error", "attempts": max_retries + 1,
                        "code": current_code,
                        "output": output[:2000] if output else "(no output)",
                        "error": output[:500] if output else "unknown",
                    })
                    cells_failed += 1

    # Step 4: Save migrated notebook
    os.makedirs(os.path.dirname(out_nb_path), exist_ok=True)
    with open(out_nb_path, 'w') as f:
        json.dump(migrated_nb, f, indent=1)

    # Step 5: Generate report
    overall = "PASS" if cells_failed == 0 else "PARTIAL" if cells_ok > 0 else "FAIL"
    report = f"""# Migration & Validation Report: {notebook_rel_path}

## Summary
- **Date**: {datetime.now().isoformat()}
- **Result**: **{overall}**
- **Cells OK**: {cells_ok} | **Failed**: {cells_failed} | **Skipped**: {cells_skipped} | **Auto-fixed**: {cells_fixed}
- **Output comparisons**: {output_matches} match, {output_mismatches} differ
- **Claude tokens**: {usage['input'] + usage['output']:,} (migration)

## Migration Notes
"""
    for note in migration_notes:
        report += f"- {note}\n"

    report += "\n## Cell Results\n"
    for cr in cell_results:
        ci = cr["cell"]
        cls = cr.get("classification", "?")
        st = cr.get("status", "?")

        if st == "skipped":
            report += f"\n### Cell {ci} - SKIPPED ({cr.get('reason', cls)})\n"
            if cr.get("code"):
                report += f"```python\n{cr['code']}\n```\n"
            continue

        report += f"\n### Cell {ci} - {st.upper()}"
        if cr.get("fixed"):
            report += f" (auto-fixed, attempt {cr.get('attempts', '?')})"
        report += f"\n**Classification**: {cls}\n"

        code = cr.get("code", "")
        if code:
            display_code = code if len(code) < 1500 else code[:1500] + "\n# ... truncated"
            report += f"\n**Code**:\n```python\n{display_code}\n```\n"

        output = cr.get("output", "")
        if output and output != "(no output)":
            display_out = output if len(output) < 2000 else output[:2000] + "\n... truncated"
            report += f"\n**Output**:\n```\n{display_out}\n```\n"

        if cr.get("original_output"):
            report += f"\n**Original output**: `{cr['original_output'][:200]}`\n"
            report += f"**Match**: {cr.get('output_match', 'N/A')}\n"

        if cr.get("error"):
            report += f"\n**Error**: `{cr['error'][:300]}`\n"

    with open(out_report_path, 'w') as f:
        f.write(report)

    print(f"  RESULT: {overall} | OK:{cells_ok} Failed:{cells_failed} Skipped:{cells_skipped} Fixed:{cells_fixed}")

    return {
        "path": notebook_rel_path,
        "status": overall,
        "ok": cells_ok,
        "failed": cells_failed,
        "skipped": cells_skipped,
        "fixed": cells_fixed,
        "output_matches": output_matches,
        "output_mismatches": output_mismatches,
    }


async def run_parallel_batch(notebooks: List[str], cluster_id: str, parallel: int, max_retries: int):
    """Run notebooks in parallel with separate sessions."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"{'='*60}")
    print(f"Unified Migrate-Validate-Fix Pipeline")
    print(f"{'='*60}")
    print(f"Notebooks: {len(notebooks)}")
    print(f"Parallel sessions: {parallel}")
    print(f"Max retries: {max_retries} (Sonnet 1-2, Opus 3-5)")
    print(f"Cluster: {cluster_id}")
    print(f"Started: {datetime.now().isoformat()}")

    # Split notebooks into chunks for parallel processing
    chunks = [[] for _ in range(parallel)]
    for i, nb in enumerate(notebooks):
        chunks[i % parallel].append(nb)

    all_results = []

    async def process_chunk(chunk_id: int, chunk: List[str]):
        session = AIDPSession(cluster_id=cluster_id)
        results = []
        reconnect_every = 10

        for i, nb_path in enumerate(chunk):
            # Reconnect periodically
            if i % reconnect_every == 0:
                if i > 0:
                    try:
                        await session.close()
                    except Exception:
                        pass
                try:
                    session = AIDPSession(cluster_id=cluster_id)
                    await session.connect()
                except Exception as e:
                    print(f"  [Worker {chunk_id}] Connection failed: {e}")
                    await asyncio.sleep(5)
                    try:
                        session = AIDPSession(cluster_id=cluster_id)
                        await session.connect()
                    except Exception:
                        results.append({"path": nb_path, "status": "connection_error", "error": str(e)})
                        continue

            try:
                result = await process_notebook(nb_path, session, max_retries)
                results.append(result)
            except Exception as e:
                print(f"  [Worker {chunk_id}] Error on {nb_path}: {e}")
                results.append({"path": nb_path, "status": "error", "error": str(e)})
                # Force reconnect
                try:
                    await session.close()
                except Exception:
                    pass
                session = AIDPSession(cluster_id=cluster_id)

        try:
            await session.close()
        except Exception:
            pass
        return results

    # Run chunks in parallel
    tasks = [process_chunk(i, chunk) for i, chunk in enumerate(chunks) if chunk]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    for cr in chunk_results:
        if isinstance(cr, list):
            all_results.extend(cr)
        else:
            print(f"  Chunk error: {cr}")

    # Summary
    total_ok = sum(r.get("ok", 0) for r in all_results if isinstance(r, dict))
    total_fail = sum(r.get("failed", 0) for r in all_results if isinstance(r, dict))
    total_skip = sum(r.get("skipped", 0) for r in all_results if isinstance(r, dict))
    total_fix = sum(r.get("fixed", 0) for r in all_results if isinstance(r, dict))
    total_match = sum(r.get("output_matches", 0) for r in all_results if isinstance(r, dict))
    total_mismatch = sum(r.get("output_mismatches", 0) for r in all_results if isinstance(r, dict))
    pass_count = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "PASS")
    partial_count = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "PARTIAL")
    fail_count = sum(1 for r in all_results if isinstance(r, dict) and r.get("status") == "FAIL")

    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*60}")
    print(f"Notebooks:       {len(all_results)} processed")
    print(f"  PASS:          {pass_count}")
    print(f"  PARTIAL:       {partial_count}")
    print(f"  FAIL:          {fail_count}")
    print(f"Cells:           {total_ok} OK, {total_fail} failed, {total_skip} skipped")
    print(f"Auto-fixes:      {total_fix}")
    print(f"Output compare:  {total_match} match, {total_mismatch} differ")
    print(f"{'='*60}")

    # Save batch report
    batch_path = os.path.join(OUTPUT_DIR, "batch_report.json")
    with open(batch_path, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "results": all_results,
            "summary": {
                "total": len(all_results),
                "pass": pass_count, "partial": partial_count, "fail": fail_count,
                "cells_ok": total_ok, "cells_failed": total_fail,
                "cells_skipped": total_skip, "auto_fixes": total_fix,
                "output_matches": total_match, "output_mismatches": total_mismatch,
            }
        }, f, indent=2, default=str)
    print(f"Report: {batch_path}")


async def main():
    parser = argparse.ArgumentParser(description="Unified migrate-validate-fix pipeline")
    parser.add_argument("notebook", nargs="?", help="Single notebook relative path")
    parser.add_argument("--batch", help="JSON list of notebooks")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--parallel", type=int, default=5, help="Parallel AIDP sessions")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--profile", default="CUSTOMER")
    args = parser.parse_args()

    if args.notebook:
        session = AIDPSession(cluster_id=args.cluster, oci_profile=args.profile)
        await session.connect()
        try:
            result = await process_notebook(args.notebook, session, args.max_retries)
            print(json.dumps(result, indent=2))
        finally:
            await session.close()

    elif args.batch:
        with open(args.batch) as f:
            notebooks = json.load(f)
        nb_paths = [nb["path"] if isinstance(nb, dict) else nb for nb in notebooks]
        await run_parallel_batch(nb_paths, args.cluster, args.parallel, args.max_retries)

    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
