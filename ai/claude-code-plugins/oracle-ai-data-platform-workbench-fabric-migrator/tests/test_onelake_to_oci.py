import unittest

from fabric_aidp.translate.onelake_to_oci import (
    PathMapping, is_onelake_path, map_onelake_path,
)

NS = "acmens"
ABFSS = "abfss://Analytics@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse/Files/raw/x.parquet"
GUID_PATH = ("abfss://ca0a79b9-9c03-40b5-9d6a-cfd3fda1c31e@onelake.dfs.fabric.microsoft.com/"
             "2925655f-0293-4f32-8bc6-86ab989099a7/Tables/claims")


class AbfssTests(unittest.TestCase):
    def test_named_item_maps_to_bucket_and_preserves_suffix(self):
        r = map_onelake_path(ABFSS, namespace=NS)
        self.assertEqual(
            r.mapped,
            "oci://Analytics_Sales_Lakehouse@acmens/Files/raw/x.parquet")
        self.assertEqual(r.lakehouse, "Sales")
        self.assertIsNone(r.reason)

    def test_guid_item_resolves_through_the_index(self):
        r = map_onelake_path(GUID_PATH, namespace=NS,
                             guid_index={"2925655f-0293-4f32-8bc6-86ab989099a7": "Sales"})
        self.assertEqual(
            r.mapped,
            "oci://ca0a79b9-9c03-40b5-9d6a-cfd3fda1c31e_Sales@acmens/Tables/claims")

    def test_unknown_guid_is_reported_not_guessed(self):
        r = map_onelake_path(GUID_PATH, namespace=NS, guid_index={})
        self.assertIsNone(r.mapped)
        self.assertIn("not in this workspace export", r.reason)

    def test_item_with_no_trailing_path_maps_to_bucket_root(self):
        r = map_onelake_path(
            "abfss://W@onelake.dfs.fabric.microsoft.com/Sales.Lakehouse", namespace=NS)
        self.assertEqual(r.mapped, "oci://W_Sales_Lakehouse@acmens")


class FuseTests(unittest.TestCase):
    def test_fuse_path_uses_the_default_lakehouse(self):
        r = map_onelake_path("/lakehouse/default/Files/raw/x.csv",
                             namespace=NS, default_lakehouse="Sales")
        self.assertEqual(r.mapped, "oci://Sales@acmens/Files/raw/x.csv")

    def test_fuse_path_without_a_binding_is_reported(self):
        r = map_onelake_path("/lakehouse/default/Files/x", namespace=NS)
        self.assertIsNone(r.mapped)
        self.assertIn("no default lakehouse", r.reason)


class RelativeTests(unittest.TestCase):
    def test_relative_files_path_uses_the_default_lakehouse(self):
        r = map_onelake_path("Files/raw/x.csv", namespace=NS, default_lakehouse="Sales")
        self.assertEqual(r.mapped, "oci://Sales@acmens/Files/raw/x.csv")

    def test_relative_tables_path(self):
        r = map_onelake_path("Tables/claims", namespace=NS, default_lakehouse="Sales")
        self.assertEqual(r.mapped, "oci://Sales@acmens/Tables/claims")


class HttpsEndpointTests(unittest.TestCase):
    """OneLake's HTTPS endpoint names exactly what the abfss form names, and
    it is the spelling Fabric's portal gives under "Copy URL" -- and the only
    one `requests`, `pandas.read_parquet`, `duckdb` or `azcopy` can be
    handed, since none of them speaks abfss.

    It was not recognised at all, so a literal in it was neither rewritten
    nor flagged. It reached the artifact untouched with no finding and the
    notebook graded PASS: a silent miss, which is the worst of the three
    outcomes this module can produce."""

    HTTPS = ("https://onelake.dfs.fabric.microsoft.com/Analytics/"
             "Sales.Lakehouse/Files/raw/x.parquet")

    def test_it_is_recognised_as_a_onelake_path(self):
        self.assertTrue(is_onelake_path(self.HTTPS))

    def test_it_maps_to_the_bucket_the_abfss_form_maps_to(self):
        self.assertEqual(map_onelake_path(self.HTTPS, namespace=NS).mapped,
                         map_onelake_path(ABFSS, namespace=NS).mapped)

    def test_the_blob_endpoint_is_the_same_location(self):
        blob = self.HTTPS.replace("onelake.dfs", "onelake.blob")
        self.assertEqual(map_onelake_path(blob, namespace=NS).mapped,
                         "oci://Analytics_Sales_Lakehouse@acmens/Files/raw/x.parquet")

    def test_an_item_guid_is_refused_exactly_as_the_abfss_form_refuses_it(self):
        path = ("https://onelake.dfs.fabric.microsoft.com/ws/"
                "2925655f-0293-4f32-8bc6-86ab989099a7/Tables/claims")
        r = map_onelake_path(path, namespace=NS, guid_index={})
        self.assertIsNone(r.mapped)
        self.assertIn("not in this workspace export", r.reason)

    def test_a_percent_encoded_workspace_is_decoded_before_the_bucket_check(self):
        path = ("https://onelake.dfs.fabric.microsoft.com/My%20WS/"
                "Sales.Lakehouse/Files/x")
        r = map_onelake_path(path, namespace=NS)
        self.assertIsNone(r.mapped)
        self.assertIn("not a valid OCI bucket name", r.reason)

    def test_another_fabric_https_url_is_not_a_onelake_path(self):
        self.assertFalse(is_onelake_path("https://app.powerbi.com/groups/me"))
        self.assertFalse(is_onelake_path(
            "https://api.fabric.microsoft.com/v1/workspaces"))


class GuidOnlyBindingTests(unittest.TestCase):
    """A Fabric Git export often records the notebook's lakehouse binding as
    a GUID and nothing else, and `default_lakehouse` returns that GUID. It is
    a legal OCI bucket name, so the two forms that take their item from the
    binding emitted it and called the result a success.

    The abfss branch already refused an unresolvable item GUID. These two did
    not, so one tool gave two answers to the same question."""

    LAKEHOUSE_GUID = "2925655f-0293-4f32-8bc6-86ab989099a7"

    def test_a_relative_path_does_not_become_a_guid_bucket(self):
        r = map_onelake_path("Files/raw/x", namespace=NS,
                             default_lakehouse=self.LAKEHOUSE_GUID)
        self.assertIsNone(r.mapped)
        self.assertIn(self.LAKEHOUSE_GUID, r.reason)

    def test_a_fuse_path_does_not_become_a_guid_bucket(self):
        r = map_onelake_path("/lakehouse/default/Files/raw/x", namespace=NS,
                             default_lakehouse=self.LAKEHOUSE_GUID)
        self.assertIsNone(r.mapped)
        self.assertIn(self.LAKEHOUSE_GUID, r.reason)

    def test_the_refusal_says_why_rather_than_only_that(self):
        r = map_onelake_path("Files/raw/x", namespace=NS,
                             default_lakehouse=self.LAKEHOUSE_GUID)
        self.assertIn("GUID", r.reason)
        self.assertIn("by hand", r.reason)

    def test_a_guid_the_index_knows_still_resolves(self):
        r = map_onelake_path("Files/raw/x", namespace=NS,
                             default_lakehouse=self.LAKEHOUSE_GUID,
                             guid_index={self.LAKEHOUSE_GUID: "Sales"})
        self.assertEqual(r.mapped, "oci://Sales@acmens/Files/raw/x")

    def test_a_named_binding_is_untouched(self):
        r = map_onelake_path("Files/raw/x", namespace=NS,
                             default_lakehouse="Sales")
        self.assertEqual(r.mapped, "oci://Sales@acmens/Files/raw/x")

    def test_a_lakehouse_merely_named_like_a_guid_prefix_is_untouched(self):
        r = map_onelake_path("Files/raw/x", namespace=NS,
                             default_lakehouse="2925655f-0293")
        self.assertEqual(r.mapped, "oci://2925655f-0293@acmens/Files/raw/x")


class NonOneLakeTests(unittest.TestCase):
    def test_s3_path_is_not_a_onelake_path(self):
        r = map_onelake_path("s3://bucket/key", namespace=NS)
        self.assertEqual((r.mapped, r.lakehouse, r.reason), (None, None, None))
        self.assertFalse(is_onelake_path("s3://bucket/key"))

    def test_plain_local_path_is_not_a_onelake_path(self):
        self.assertFalse(is_onelake_path("/tmp/data.csv"))

    def test_is_onelake_path_true_for_each_recognised_form(self):
        for p in (ABFSS, GUID_PATH, "/lakehouse/default/Files/x", "Files/x", "Tables/t"):
            with self.subTest(path=p):
                self.assertTrue(is_onelake_path(p))


class ValidationTests(unittest.TestCase):
    def test_lakehouse_name_invalid_as_a_bucket_is_flagged_not_mangled(self):
        r = map_onelake_path(
            "abfss://W@onelake.dfs.fabric.microsoft.com/My Sales.Lakehouse/Files/x",
            namespace=NS)
        self.assertIsNone(r.mapped)
        self.assertEqual(r.lakehouse, "My Sales")
        self.assertIn("not a valid OCI bucket name", r.reason)

    def test_percent_encoded_name_decodes_before_validation(self):
        r = map_onelake_path(
            "abfss://W@onelake.dfs.fabric.microsoft.com/My%20Sales.Lakehouse/Files/x",
            namespace=NS)
        self.assertIsNone(r.mapped)
        self.assertEqual(r.lakehouse, "My Sales")

    def test_invalid_namespace_is_rejected(self):
        with self.assertRaises(ValueError):
            map_onelake_path(ABFSS, namespace="Bad Namespace!")

    def test_non_string_input_is_not_a_path(self):
        self.assertFalse(is_onelake_path(None))
        self.assertEqual(map_onelake_path(None, namespace=NS), PathMapping(None, None, None))


if __name__ == "__main__":
    unittest.main()
