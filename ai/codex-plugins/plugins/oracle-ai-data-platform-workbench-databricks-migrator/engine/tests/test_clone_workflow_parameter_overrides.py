"""Regression coverage for forwarding migration parameters to cloned jobs."""

import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from clone_workflow import apply_parameter_overrides, parse_parameter_overrides


def test_applies_matching_task_parameters_and_reports_unmatched_values():
    payload = {
        "parameters": [],
        "tasks": [
            {
                "taskKey": "DataProxy_O1_FT",
                "parameters": [
                    {"name": "table_name", "value": ""},
                    {"name": "fl_version", "value": ""},
                ],
            },
            {
                "taskKey": "O1_FL_Deletion",
                "parameters": [{"name": "deletion_data", "value": "NaN"}],
            },
        ],
    }
    overrides = {
        "table_name": "policy_framework_db.temp_blizzard_hit_data_part4",
        "fl_version": "177",
        "deletion_data": "NaN",
        "start_date": "2025-12-31",
    }

    applied, unmatched = apply_parameter_overrides(payload, overrides)

    assert payload["tasks"][0]["parameters"] == [
        {
            "name": "table_name",
            "value": "policy_framework_db.temp_blizzard_hit_data_part4",
        },
        {"name": "fl_version", "value": "177"},
    ]
    assert payload["tasks"][1]["parameters"] == [
        {"name": "deletion_data", "value": "NaN"}
    ]
    assert payload["parameters"] == [
        {
            "name": "table_name",
            "value": "policy_framework_db.temp_blizzard_hit_data_part4",
        },
        {"name": "fl_version", "value": "177"},
        {"name": "deletion_data", "value": "NaN"},
        {"name": "start_date", "value": "2025-12-31"},
    ]
    assert len(applied) == 7
    assert unmatched == []


def test_parse_parameter_overrides_splits_only_first_equals():
    assert parse_parameter_overrides(["query=a=b", "fl_version=177"]) == {
        "query": "a=b",
        "fl_version": "177",
    }
