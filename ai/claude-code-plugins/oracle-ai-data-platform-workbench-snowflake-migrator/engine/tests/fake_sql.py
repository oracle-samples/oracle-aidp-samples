"""A run_sql test double. Matches canned responses by SQL substring."""
from __future__ import annotations


class FakeSql:
    def __init__(self, responses: dict[str, list[dict]]):
        self.responses = responses
        self.calls: list[str] = []

    def __call__(self, sql: str, params: dict | None = None) -> list[dict]:
        self.calls.append(sql)
        flat = " ".join(sql.split()).lower()
        for needle, rows in self.responses.items():
            if needle.lower() in flat:
                return rows
        raise AssertionError(f"FakeSql has no canned response for: {flat[:160]}")
