import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fabric_aidp.inventory import lakehouse as lh
from fabric_aidp.inventory.git_workspace import discover_items

S3_SHORTCUT = {
    "path": "Tables", "name": "claims_raw",
    "target": {"type": "AmazonS3",
               "amazonS3": {"location": "https://acme.s3.us-east-1.amazonaws.com",
                            "subpath": "/claims"}},
}
ADLS_SHORTCUT = {
    "path": "Files", "name": "adls_drop",
    "target": {"type": "AdlsGen2",
               "adlsGen2": {"location": "https://acct.dfs.core.windows.net",
                            "subpath": "/landing"}},
}
# Microsoft's own Create Shortcut example. `location` is the endpoint with no
# bucket in it, and the bucket is a sibling field -- the one target type where
# the URI alone cannot say which bucket the data is in.
S3_COMPATIBLE_SHORTCUT = {
    "path": "Files", "name": "minio_claims",
    "target": {"type": "S3Compatible",
               "s3Compatible": {"location": "https://s3endpoint.contoso.com",
                                "bucket": "contosoBucket1",
                                "subpath": "/s3CompatibleDirectory/data"}},
}
ONELAKE_SHORTCUT = {
    "path": "Tables", "name": "shared_dim",
    "target": {"type": "OneLake",
               "oneLake": {"workspaceId": "ws-1", "itemId": "item-1",
                           "path": "Tables/dim_date"}},
}


def _lakehouse(root: Path, display: str, shortcuts=None, alm=None, dar=None) -> Path:
    d = root / f"{display}.Lakehouse"
    d.mkdir(parents=True)
    (d / ".platform").write_text(json.dumps({
        "config": {"logicalId": f"id-{display}"},
        "metadata": {"type": "Lakehouse", "displayName": display},
    }), encoding="utf-8")
    if shortcuts is not None:
        (d / "shortcuts.metadata.json").write_text(json.dumps(shortcuts), encoding="utf-8")
    if alm is not None:
        (d / "alm.settings.json").write_text(json.dumps(alm), encoding="utf-8")
    if dar is not None:
        (d / "data-access-roles.json").write_text(json.dumps(dar), encoding="utf-8")
    return d


class TargetTests(unittest.TestCase):
    def test_s3_target_joins_location_and_subpath(self):
        kind, uri, external = lh.shortcut_target(S3_SHORTCUT["target"])
        self.assertEqual(kind, "AmazonS3")
        self.assertEqual(uri, "https://acme.s3.us-east-1.amazonaws.com/claims")
        self.assertTrue(external)

    def test_adls_target(self):
        _, uri, external = lh.shortcut_target(ADLS_SHORTCUT["target"])
        self.assertEqual(uri, "https://acct.dfs.core.windows.net/landing")
        self.assertTrue(external)

    def test_onelake_target_is_internal(self):
        kind, uri, external = lh.shortcut_target(ONELAKE_SHORTCUT["target"])
        self.assertEqual(kind, "OneLake")
        self.assertFalse(external)
        self.assertIn("item-1", uri)

    def test_unknown_target_type_yields_no_uri(self):
        kind, uri, external = lh.shortcut_target({"type": "SomeFutureStore", "x": {}})
        self.assertEqual(kind, "SomeFutureStore")
        self.assertEqual(uri, "")
        self.assertTrue(external)

    def test_malformed_target_does_not_raise(self):
        self.assertEqual(lh.shortcut_target(None), ("", "", False))
        self.assertEqual(lh.shortcut_target({})[1], "")

    def test_an_s3_compatible_bucket_is_read_from_its_own_field(self):
        self.assertEqual(
            lh.shortcut_bucket(S3_COMPATIBLE_SHORTCUT["target"]), "contosoBucket1")

    def test_a_target_type_with_no_bucket_field_reports_none(self):
        self.assertEqual(lh.shortcut_bucket(S3_SHORTCUT["target"]), "")
        self.assertEqual(lh.shortcut_bucket(ONELAKE_SHORTCUT["target"]), "")
        self.assertEqual(lh.shortcut_bucket(None), "")

    def test_the_s3_compatible_uri_is_the_endpoint_and_subpath(self):
        """The bucket stays out of the URI: `location` is documented as the
        non-bucket-specific endpoint, so its path is the subpath."""
        _, uri, external = lh.shortcut_target(S3_COMPATIBLE_SHORTCUT["target"])
        self.assertEqual(
            uri, "https://s3endpoint.contoso.com/s3CompatibleDirectory/data")
        self.assertTrue(external)

    def test_double_slash_is_not_produced(self):
        _, uri, _ = lh.shortcut_target(
            {"type": "AmazonS3", "amazonS3": {"location": "https://h/", "subpath": "/p"}})
        self.assertEqual(uri, "https://h/p")


class CoverageTests(unittest.TestCase):
    def _coverage(self, alm):
        with TemporaryDirectory() as t:
            root = Path(t)
            _lakehouse(root, "Sales", shortcuts=[], alm=alm)
            return lh.tracking_coverage(discover_items(root)[0])

    def test_absent_settings_file_means_unknown_not_zero(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            _lakehouse(root, "Sales", shortcuts=[])
            cov = lh.tracking_coverage(discover_items(root)[0])
        self.assertEqual(cov["shortcuts"], "unknown")
        self.assertEqual(cov["data_access_roles"], "unknown")

    def test_tracked_type_is_reported_tracked(self):
        cov = self._coverage({"trackedObjectTypes": ["Shortcuts"]})
        self.assertEqual(cov["shortcuts"], "tracked")

    def test_type_absent_from_an_explicit_list_is_not_tracked(self):
        cov = self._coverage({"trackedObjectTypes": ["Shortcuts"]})
        self.assertEqual(cov["data_access_roles"], "not_tracked")

    def test_matching_is_case_and_separator_insensitive(self):
        cov = self._coverage({"trackedObjectTypes": ["data_access_roles", "shortcuts"]})
        self.assertEqual(cov["shortcuts"], "tracked")
        self.assertEqual(cov["data_access_roles"], "tracked")

    def test_unrecognised_settings_shape_degrades_to_unknown(self):
        cov = self._coverage({"somethingElse": True})
        self.assertEqual(cov["shortcuts"], "unknown")

    def test_malformed_settings_json_degrades_to_unknown(self):
        with TemporaryDirectory() as t:
            root = Path(t)
            d = _lakehouse(root, "Sales", shortcuts=[])
            (d / "alm.settings.json").write_text("{not json", encoding="utf-8")
            cov = lh.tracking_coverage(discover_items(root)[0])
        self.assertEqual(cov["shortcuts"], "unknown")


class ScanTests(unittest.TestCase):
    def _scan(self, build):
        with TemporaryDirectory() as t:
            root = Path(t)
            build(root)
            return lh.scan(discover_items(root))

    def test_shortcuts_are_collected_with_section_and_target(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts=[S3_SHORTCUT, ONELAKE_SHORTCUT],
            alm={"trackedObjectTypes": ["Shortcuts"]}))
        shortcuts = out["items"]["lakehouses"][0]["shortcuts"]
        self.assertEqual([s["name"] for s in shortcuts], ["claims_raw", "shared_dim"])
        self.assertEqual(shortcuts[0]["section"], "Tables")
        self.assertTrue(shortcuts[0]["external"])
        self.assertFalse(shortcuts[1]["external"])

    def test_wrapped_object_form_is_accepted(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts={"shortcuts": [S3_SHORTCUT]}))
        self.assertEqual(len(out["items"]["lakehouses"][0]["shortcuts"]), 1)

    def test_untracked_lakehouse_reports_unknown_not_zero(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", alm={"trackedObjectTypes": []}))
        record = out["items"]["lakehouses"][0]
        self.assertEqual(record["tracking"]["shortcuts"], "not_tracked")
        self.assertIsNone(record["shortcut_count"])
        self.assertEqual(out["summary"]["coverage_unknown_count"], 1)

    def test_tracked_lakehouse_with_no_shortcuts_reports_zero(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts=[], alm={"trackedObjectTypes": ["Shortcuts"]}))
        record = out["items"]["lakehouses"][0]
        self.assertEqual(record["shortcut_count"], 0)
        self.assertEqual(out["summary"]["coverage_unknown_count"], 0)

    def test_summary_counts_external_shortcuts_separately(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts=[S3_SHORTCUT, ADLS_SHORTCUT, ONELAKE_SHORTCUT],
            alm={"trackedObjectTypes": ["Shortcuts"]}))
        self.assertEqual(out["summary"]["shortcut_count"], 3)
        self.assertEqual(out["summary"]["external_shortcut_count"], 2)

    def test_malformed_shortcuts_file_is_recorded_not_fatal(self):
        def build(root):
            d = _lakehouse(root, "Sales")
            (d / "shortcuts.metadata.json").write_text("{not json", encoding="utf-8")
        out = self._scan(build)
        record = out["items"]["lakehouses"][0]
        self.assertEqual(record["shortcuts"], [])
        self.assertIn("shortcuts_error", record)

    def test_a_scanned_s3_compatible_shortcut_carries_its_bucket(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts=[S3_COMPATIBLE_SHORTCUT],
            alm={"trackedObjectTypes": ["Shortcuts"]}))
        record = out["items"]["lakehouses"][0]["shortcuts"][0]
        self.assertEqual(record["bucket"], "contosoBucket1")

    def test_records_for_other_types_carry_an_empty_bucket(self):
        out = self._scan(lambda r: _lakehouse(
            r, "Sales", shortcuts=[S3_SHORTCUT],
            alm={"trackedObjectTypes": ["Shortcuts"]}))
        self.assertEqual(out["items"]["lakehouses"][0]["shortcuts"][0]["bucket"], "")

    def test_no_lakehouses_yields_an_empty_but_valid_shape(self):
        self.assertEqual(lh.scan([]), {
            "summary": {"lakehouse_count": 0, "shortcut_count": 0,
                        "external_shortcut_count": 0, "coverage_unknown_count": 0},
            "items": {"lakehouses": []},
        })


class UnreadableShortcutsTests(unittest.TestCase):
    """A shortcuts file that could not be read must not report a count.

    Same class as the `.platform` defect: a silent zero is indistinguishable
    from a real zero, and here it was worse than silent -- the log said
    "tracked (0 found)", which claims the file was read and was empty.

    Every case below has `alm.settings.json` saying shortcuts ARE tracked,
    which is what made the count look trustworthy.
    """

    TRACKED_ALM = {"objectTypes": [{"name": "Shortcuts", "state": "Enabled"}]}

    def _scan(self, body):
        with TemporaryDirectory() as t:
            root = Path(t)
            d = _lakehouse(root, "Sales", alm=self.TRACKED_ALM)
            if body is not None:
                (d / "shortcuts.metadata.json").write_text(body, encoding="utf-8")
            lines = []
            out = lh.scan(discover_items(root), log=lines.append)
            return out, out["items"]["lakehouses"][0], lines

    def _assert_not_a_zero(self, record, lines):
        self.assertIsNone(record["shortcut_count"])
        self.assertEqual(record["tracking"]["shortcuts"], "unknown")
        self.assertIn("shortcuts_error", record)
        self.assertNotIn("0 found", lines[0])

    def test_malformed_json_does_not_report_zero(self):
        out, record, lines = self._scan("{not json")
        self._assert_not_a_zero(record, lines)
        self.assertEqual(out["summary"]["coverage_unknown_count"], 1)

    def test_valid_json_that_is_not_a_list_does_not_report_zero(self):
        _out, record, lines = self._scan('{"foo": 1}')
        self._assert_not_a_zero(record, lines)
        self.assertIn("shortcuts", record["shortcuts_error"])

    def test_valid_json_that_is_a_scalar_does_not_report_zero(self):
        for body in ("42", '"hello"', "null"):
            with self.subTest(body=body):
                _out, record, lines = self._scan(body)
                self._assert_not_a_zero(record, lines)

    def test_an_absent_file_while_tracking_is_on_does_not_report_zero(self):
        """Fabric spells "no shortcuts" as `[]`, not as a missing file.

        Evidence: the vendored real export Diabetes_LH.Lakehouse has
        shortcuts enabled in alm.settings.json and ships a
        shortcuts.metadata.json containing `[]`. So an absent file while
        tracking is on is an incomplete export, not a zero.
        """
        _out, record, lines = self._scan(None)
        self._assert_not_a_zero(record, lines)
        self.assertIn("missing", record["shortcuts_error"])

    def test_a_real_empty_list_is_still_a_real_zero(self):
        """The case the fix must not swallow."""
        out, record, lines = self._scan("[]")
        self.assertEqual(record["shortcut_count"], 0)
        self.assertEqual(record["tracking"]["shortcuts"], "tracked")
        self.assertNotIn("shortcuts_error", record)
        self.assertIn("0 found", lines[0])
        self.assertEqual(out["summary"]["coverage_unknown_count"], 0)

    def test_an_absent_file_with_tracking_off_is_still_not_an_error(self):
        """Tracking disabled is a known reason for no file, already handled."""
        with TemporaryDirectory() as t:
            root = Path(t)
            _lakehouse(root, "Sales", alm={"trackedObjectTypes": []})
            record = lh.scan(discover_items(root))["items"]["lakehouses"][0]
        self.assertEqual(record["tracking"]["shortcuts"], "not_tracked")
        self.assertIsNone(record["shortcut_count"])
        self.assertNotIn("shortcuts_error", record)


class ZeroGuidShortcutTests(unittest.TestCase):
    """Fabric writes an all-zero GUID to mean "this item, this workspace".

    Passed through literally the drafted path read
    `onelake://00000000-.../00000000-.../Files/raw`, which names nothing and
    cannot be reviewed. Found by the sibling project on a live tenant.
    """

    ZERO = "00000000-0000-0000-0000-000000000000"

    def _target(self, workspace, item, this_item="lh-abc"):
        return lh.shortcut_target(
            {"type": "OneLake", "oneLake": {
                "workspaceId": workspace, "itemId": item, "path": "Files/raw"}},
            this_item=this_item)[1]

    def test_zero_item_resolves_to_the_containing_lakehouse(self):
        self.assertIn("lh-abc", self._target(self.ZERO, self.ZERO))

    def test_zero_workspace_is_named_not_printed_as_zeros(self):
        uri = self._target(self.ZERO, self.ZERO)
        self.assertNotIn(self.ZERO, uri)
        self.assertIn("<this-workspace>", uri)

    def test_a_real_guid_is_left_exactly_as_written(self):
        uri = self._target("ws-1", "it-1")
        self.assertEqual(uri, "onelake://ws-1/it-1/Files/raw")

    def test_an_empty_item_still_yields_no_uri(self):
        self.assertEqual(self._target("ws-1", "", this_item=None), "")


if __name__ == "__main__":
    unittest.main()
