---
name: geo_skill
description: Answer country-capital questions.
allowed-tools:
  - run_skill_entrypoint
entrypoints:
  - name: capital_lookup
    script: scripts/capitals.py
    func: get_capital
    description: Return capital for a country.
---

Use this skill for geography lookups.
