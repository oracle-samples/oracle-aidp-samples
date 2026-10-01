---
description: Translate a plan into reviewable Spark artifacts (offline).
---

```bash
fabric-aidp migrate plan.json -o ./migrated
```

`migrate` is offline and writes only under `-o`. It translates Fabric notebooks
to Spark, Warehouse T-SQL to Spark SQL, Power Query M to PySpark, pipelines to
AIDP workflow jobs, and shortcut targets to `oci://` locations, alongside
`report.json`, `report.md` and `report.html`.

Nothing in this verb reaches AIDP. `publish` is the one verb that writes to a
workspace: it is a dry run until you add `--apply`, and it never overwrites
anything it did not create.

There is **no mode flag**. `--demo` used to be required, then was accepted and
ignored, and is now removed — `fabric-aidp migrate plan.json --demo` exits 2 with
"unrecognized arguments". If a user's script passes it, tell them to delete the
flag; nothing else about the command changes.

**Dataflows need Node.** Power Query has no Python parser, so without
`npm install` in `fabric_aidp/mparse/` every Dataflow is reported as counted but
not translated rather than migrated. If the user expected PySpark out of their
Dataflows and got none, check that before anything else.

`--filter <slice>` migrates one slice: `notebook`, `warehouse`, `lakehouse`,
`pipeline`, `semanticmodel` or `dataflow`. A refusal that belongs to no slice —
an item no scanner could read or claim — is reported whatever the filter is,
because no filter could otherwise show it.

**`migrate` never deletes anything under `-o`.** A second run into the same
directory leaves the first run's artifacts there, and the new `report.json`
does not vouch for them. That is deliberate: migrating slices into one
directory is a workflow this tool recommends. It is also why the report lists
those files under `unclaimed_artifacts`, `report.md` ends with a **Not written
by this run** section, and `verify` prints them. `publish` reads the report, so
it will not send them — but a user reading or globbing the directory has no
such protection, so tell them which files the report disowns. Migrating into an
empty directory is the way to get one that describes itself.

Report back the ok / needs-review / **blocked** / planned counts — all four.
`blocked` is an object the tool refused to translate rather than translate
partly, so it is work the user still has to do; leaving it out of the summary
makes a migration look more finished than it is. Then walk the user through the
REVIEW findings. Do **not** edit a flagged construct into a silent rewrite — a
flag means the two engines genuinely differ, and the decision belongs to the
user.
