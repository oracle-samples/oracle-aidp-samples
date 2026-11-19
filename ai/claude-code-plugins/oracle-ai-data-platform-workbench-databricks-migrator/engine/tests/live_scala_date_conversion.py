"""Opt-in live test of ONLY Scala date collection -> model repair -> execution.

Uses synthetic dates, the real call_fix/submit_code path and AIDPSession. It
neither launches a customer job nor starts/stops/configures the chosen cluster.
Requires an ACTIVE operator-selected cluster, OCI auth and the configured LLM.
Output directory must not exist. Session artifacts are retained for inspection.
This does not validate workflow orchestration; see PR #242 for that requirement:
the source toolkit (PR 242)

python tests/live_scala_date_conversion.py --config <yaml> --cluster <key> \
    --output-dir /tmp/scala-date-conversion-<unique-name>
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import json
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'scripts')]

import agent_migrate as am
import aidp_executor as ae
import llm_config
from aidp_config import load_config

ORIGINAL = '''%scala
val latestDate = js1.withColumn("date", col("date").cast(DateType))
  .agg(max("date").alias("latestDate"))
  .as[java.sql.Date].collect()(0).toString
'''
SETUP = '''%scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType}
import spark.implicits._
val js1 = Seq("2026-10-01", "2026-10-06", "2026-09-30").toDF("date")
'''


def failed(result):
    text = ae.format_outputs(result.get('outputs', []))
    return (result.get('status') not in (None, 'ok')
            or any(o.get('type') == 'error' for o in result.get('outputs', []))
            or 'Failing command:' in text
            or ('Command ID ' in text and ' failed with ' in text))


async def validate(config, cluster, output_dir):
    output_dir.mkdir(parents=True, exist_ok=False)
    cfg = load_config(cli_config_path=config)
    llm_config.configure(cfg)
    session = ae.AIDPSession(
        lake_ocid=cfg.aidp.lake_ocid, workspace_id=cfg.aidp.workspace_id,
        cluster_id=cluster, oci_profile=cfg.aidp.oci_profile,
        session_name='scala_date_conversion_' + uuid.uuid4().hex[:12])
    evidence = {'status': 'FAIL', 'cases': [], 'model': llm_config.MODEL}
    # Executor diagnostics can include handshake internals. Do not persist or
    # echo them. Emit only the controlled evidence below, never credentials.
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            try:
                await session.connect()
                setup = await session.execute(SETUP, timeout=180)
                assert not failed(setup), 'Synthetic date setup failed'
                original = await session.execute(ORIGINAL, timeout=180)
                error = ae.format_outputs(original.get('outputs', []))
                evidence['original_failure_reproduced'] = failed(original) and all(
                    word in error for word in ('IllegalAccessError', 'ZoneInfo'))
                assert evidence['original_failure_reproduced'], 'Expected client date-decoding error not reproduced'
                repaired = await am.call_fix(
                    ORIGINAL, error, [SETUP], 1, session=session,
                    inline_context=True,
                    extra_context='The intended result latestDate is a String. Preserve missing-date failure behaviour.')
                (output_dir / 'repaired.scala').write_text(repaired)
                # Re-run the generated cell itself for each dataset; assertions
                # live outside the model response so the model cannot remove them.
                cases = [
                    ('dates', SETUP, False),
                    ('mixed_null', '%scala\nval js1 = Seq[String](null, "2026-10-06", "2026-09-30").toDF("date")', False),
                    ('empty', '%scala\nval js1 = Seq.empty[String].toDF("date")', True),
                    ('all_null', '%scala\nval js1 = Seq[String](null, null).toDF("date")', True),
                ]
                for name, seed, expect_error in cases:
                    assert not failed(await session.execute(seed, timeout=180)), 'Dataset setup failed: ' + name
                    result = await session.execute(repaired, timeout=180)
                    text = ae.format_outputs(result.get('outputs', []))
                    if expect_error:
                        # Must fail for missing data, not the unresolved JVM error.
                        ok = failed(result) and 'IllegalAccessError' not in text and any(
                            word in text for word in ('IllegalArgumentException', 'NullPointerException', 'IllegalStateException', 'NoSuchElementException'))
                    else:
                        check = await session.execute(
                            '%scala\nassert(latestDate == "2026-10-06")\nprintln("DATE_CONVERSION_VERIFIED")', timeout=180)
                        ok = not failed(result) and not failed(check) and 'DATE_CONVERSION_VERIFIED' in ae.format_outputs(check.get('outputs', []))
                    evidence['cases'].append({'name': name, 'passed': ok, 'expected_error': expect_error})
                    assert ok, 'Conversion case failed: ' + name
                evidence['status'] = 'PASS'
            finally:
                await session.close(delete_session=False)
    finally:
        (output_dir / 'result.json').write_text(json.dumps(evidence, indent=2))
        print(json.dumps(evidence, indent=2))
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--cluster', required=True)
    parser.add_argument('--output-dir', required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(validate(args.config, args.cluster, args.output_dir))


if __name__ == '__main__':
    main()
