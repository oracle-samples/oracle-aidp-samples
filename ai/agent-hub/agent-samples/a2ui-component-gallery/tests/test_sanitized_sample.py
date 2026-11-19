from __future__ import annotations

import unittest
from pathlib import Path


SAMPLE_ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".json", ".md", ".py", ".txt"}
FORBIDDEN_MARKERS = {
    "ocid" + "1.": "OCI OCID",
    '"workspace' + 'Key"': "workspace key",
    '"compute' + 'Key"': "compute key",
    ".datalake.oci." + "oraclecloud.com": "private Data Lake host",
    "@oracle" + ".com": "Oracle user email",
}


class SanitizedSampleTests(unittest.TestCase):
    def test_private_deployment_config_is_not_committed(self):
        self.assertFalse((SAMPLE_ROOT / "deployment_config.py").exists())
        self.assertTrue((SAMPLE_ROOT / "deployment_config.example.py").is_file())

    def test_sample_contains_no_environment_specific_identifiers(self):
        violations: list[str] = []
        for path in SAMPLE_ROOT.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            text = path.read_text(encoding="utf-8")
            for marker, description in FORBIDDEN_MARKERS.items():
                if marker.lower() in text.lower():
                    violations.append(f"{path.relative_to(SAMPLE_ROOT)}: {description}")
        self.assertEqual([], violations)

    def test_requirements_have_supported_minimum_versions_without_langchain(self):
        requirements = (SAMPLE_ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertIn("jsonschema>=4.18.0", requirements)
        self.assertIn("referencing>=0.30.0", requirements)
        self.assertNotIn("langchain", requirements.lower())

    def test_agent_fails_early_for_the_compartment_placeholder(self):
        agent_source = (SAMPLE_ROOT / "agent.py").read_text(encoding="utf-8")
        self.assertIn("OCI compartment is not configured", agent_source)
        self.assertIn('configured_compartment_id == "<your-compartment-ocid>"', agent_source)


if __name__ == "__main__":
    unittest.main()
