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
PYTHONPATH=engine python3 -m infa2aidp.cli discover \
  --host <powercenter-host> \
  --port 7343 \
  --method auto \
  -o ./crawl_output
```

The Web Services Hub is reached over `https://` on every port -- the first
SOAP call is the LoginRequest, whose body is the repository password. A hub
on a non-standard HTTPS port takes `--wsh-url https://host:8443/wsh/services`
(or `INFA_WSH_URL`). Informatica's own default hub is HTTP on 7333; sending
the password over cleartext needs an explicit `--insecure-http` (or
`INFA_WSH_ALLOW_HTTP=1`) and is logged as a WARNING -- without it the SOAP
attempt is refused before anything is sent and `--method auto` goes on to
pmrep. The opt-in permits cleartext, it does not force it: with `--port 7333`
the hub URL becomes `http://`, on the 7343 default and every other port it
stays `https://`, and any other cleartext hub must be named in full with
`--wsh-url http://host:port/wsh/services`. Only agree to `--insecure-http`
for a lab host.

`--user`/`--repo`/`--domain` exist as flags but the environment variables
are the recommended form; never ask the user to paste a password into chat.
There is deliberately no working `--password` flag: passing one is refused
with exit code 2 and the message "Do not pass passwords in argv. Use
--password-file or INFA_PASSWORD." -- argv is visible in `ps`, shell history
and CI logs. If the password cannot live in the environment, write it to a
file only its owner can read (`chmod 600`) and pass `--password-file <path>`.
Verbose output names the host, repository and the password's source, never
the password itself.

## Flags

| Flag | Default | Notes |
|---|---|---|
| `--host` | required | PowerCenter host |
| `--port` | `7343` | Web Services Hub HTTPS port. 7333 is Informatica's HTTP port and needs `--insecure-http`. |
| `--wsh-url` | `INFA_WSH_URL` env var | Exact hub URL (`https://host:8443/wsh/services`) when host:port is not enough. `https://` unless `--insecure-http`; the only way to reach a cleartext hub on a port other than 7333. |
| `--insecure-http` | off (`INFA_WSH_ALLOW_HTTP=1`) | Permit cleartext `http://` to the hub: an `http://` `--wsh-url` is accepted and `--port 7333` builds an `http://` URL; the 7343 default and other ports stay `https://`. The repository password travels in the clear; logged as a WARNING. Lab hosts only. |
| `--user` | `INFA_USER` env var | |
| `--password-file` | `INFA_PASSWORD` env var | Path of an owner-only (`chmod 600`) file holding the password; group/world-readable files are refused on POSIX (check skipped on Windows). `--password <value>` is refused outright. |
| — | `INFA_CA_BUNDLE` / `INFA_TLS_VERIFY` env vars | The hub is always `https://` (above) and the certificate is verified by default. Point `INFA_CA_BUNDLE` at a corporate CA bundle; set `INFA_TLS_VERIFY=0` only for a self-signed lab host. |
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
