---
name: infa-discover
description: Extract mappings and workflows from a live Informatica PowerCenter repository via `infa2aidp cli discover` (SOAP or pmrep). Use when the user wants to pull assets directly from a running PowerCenter installation rather than starting from an export file they already have. Does not talk to IDMC/IICS — that source format only ever arrives as an export file (see infa-analyze / infa-migrate-mapping).
---

# `infa-discover` — crawl a live PowerCenter repository

The only command in this toolkit that connects to a live source system. If
the user already has an export (PowerCenter XML or IDMC/IICS JSON) on disk,
skip this and go straight to
[`infa-analyze`](../infa-analyze/SKILL.md).

## When to use

- The user says "pull our mappings from PowerCenter", "crawl the
  repository", "connect to our Informatica domain".
- The user does *not* have export files yet and the source is PowerCenter
  (not IDMC/IICS — this command is PowerCenter-only; IDMC exports come from
  the IICS Asset Management CLI outside this tool, per).

## Canonical invocation

```bash
# INFA_USER / INFA_PASSWORD / INFA_REPO / INFA_DOMAIN set in the environment
# (never on the command line, where they land in shell history and `ps`)
PYTHONPATH="${INFA_ENGINE:-$HOME/.aidp-infa-migrator/engine}" python3 -m infa2aidp.cli discover \
  --host <powercenter-host> \
  --port 6005 \
  --method auto \
  -o ./crawl_output
```

`--user`/`--password`/`--repo`/`--domain` exist as flags but the
environment variables are the recommended form; never ask the user to paste
a password into chat.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `--host` | required | PowerCenter host |
| `--port` | `6005` | |
| `--user` | `INFA_USER` env var | |
| `--password` | `INFA_PASSWORD` env var | |
| `--repo` | `INFA_REPO` env var | Repository name |
| `--domain` | `INFA_DOMAIN` env var | |
| `--method` | `auto` | `auto`, `soap`, or `pmrep` — `auto` picks whichever is reachable |
| `--folders` | all folders | Comma-separated folder list to scope the crawl |
| `-o, --output` | `./crawl_output` | |

## What it produces

```
<output>/
  exported_xml/            ← one PowerCenter XML export per crawled object
  infa_inventory_report.md ← summary: mapping/workflow/XML counts, any crawl errors
```

Console output: `<N> mappings, <M> workflows, <K> XMLs exported -> <report path>`.

## When it goes wrong

- **Connection refused / SOAP fault** — confirm the host, port, and domain;
  try `--method pmrep` if SOAP access is restricted, or vice versa.
- **Exit code 1 with no exported XML** — the crawl hit errors on every
  object it tried. Check `infa_inventory_report.md` for the first 5 logged
  errors (the command surfaces them, doesn't just swallow them).
- **Partial export (exit 0, but the report lists errors)** — some objects
  failed individually; the crawl still reports success because at least one
  XML was exported. Read the report before assuming the crawl was clean.

## After this

Point [`infa-analyze`](../infa-analyze/SKILL.md) at
`<output>/exported_xml/`.
