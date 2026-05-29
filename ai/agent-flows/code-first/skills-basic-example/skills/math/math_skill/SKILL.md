---
name: math_skill
description: Perform arithmetic calculations.
allowed-tools:
  - run_skill_entrypoint
entrypoints:
  - name: add
    script: scripts/calculator.py
    func: add_numbers
    description: Add two numbers.
  - name: multiply
    script: scripts/calculator.py
    func: multiply_numbers
    description: Multiply two numbers.
---

Use this skill for arithmetic.
