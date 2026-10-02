import json
import unittest
from pathlib import Path

from fabric_aidp.inventory.lakehouse import (
    DOCUMENTED_ONLY, FROM_AUTHORED_FIXTURE, FROM_REAL_EXPORT, SHAPE_PROVENANCE,
    _EXTERNAL_TARGETS)
from fabric_aidp.translate.shortcut_to_oci import map_target, translate

ROOT = Path(__file__).resolve().parent.parent

NS = "acmens"


def _shortcut(**over):
    base = {"name": "claims_raw", "section": "Tables", "target_type": "AmazonS3",
            "target": "https://acme.s3.us-east-1.amazonaws.com/claims",
            "external": True}
    base.update(over)
    return base


class MapTargetTests(unittest.TestCase):
    def test_s3_virtual_host_style(self):
        uri, reason = map_target(
            "AmazonS3", "https://acme.s3.us-east-1.amazonaws.com/claims", namespace=NS)
        self.assertEqual(uri, "oci://acme@acmens/claims")
        self.assertIsNone(reason)

    def test_s3_scheme_style(self):
        uri, _ = map_target("AmazonS3", "s3://acme/claims", namespace=NS)
        self.assertEqual(uri, "oci://acme@acmens/claims")

    def test_adls_gen2(self):
        # `acct_` is the storage account: an Azure container name is unique
        # only inside one account, so the bare container would collide.
        # StorageAccountTests below is where that is argued out.
        uri, _ = map_target(
            "AdlsGen2", "https://acct.dfs.core.windows.net/raw/landing", namespace=NS)
        self.assertEqual(uri, "oci://acct_raw@acmens/landing")

    def test_azure_blob(self):
        uri, _ = map_target(
            "AzureBlobStorage", "https://acct.blob.core.windows.net/raw/x", namespace=NS)
        self.assertEqual(uri, "oci://acct_raw@acmens/x")

    def test_google_cloud_storage(self):
        uri, _ = map_target(
            "GoogleCloudStorage", "https://storage.googleapis.com/bkt/x", namespace=NS)
        self.assertEqual(uri, "oci://bkt@acmens/x")

    def test_container_root_with_no_key(self):
        uri, _ = map_target("AmazonS3", "s3://acme", namespace=NS)
        self.assertEqual(uri, "oci://acme@acmens")

    def test_onelake_target_is_flagged(self):
        uri, reason = map_target("OneLake", "onelake://ws/item/Tables/t", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("internal", reason.lower())

    def test_unknown_target_type_is_flagged(self):
        uri, reason = map_target("Dataverse", "https://x", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("Dataverse", reason)

    def test_unparseable_uri_is_flagged(self):
        uri, reason = map_target("AmazonS3", "not a uri", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("could not", reason.lower())

    def test_empty_uri_is_flagged(self):
        uri, reason = map_target("AmazonS3", "", namespace=NS)
        self.assertIsNone(uri)

    def test_container_name_invalid_for_oci_is_flagged(self):
        uri, reason = map_target("AmazonS3", "s3://Not_A Valid/x", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("bucket name", reason.lower())


class StorageAccountTests(unittest.TestCase):
    """An Azure container name is unique only inside its storage account.

    Two accounts each holding a container called `data` or `raw` is an
    ordinary enterprise shape, and the account has to reach the OCI name or
    the two shortcuts land on one bucket.
    """

    def test_two_adls_accounts_do_not_collide_on_one_bucket(self):
        a, _ = map_target(
            "AdlsGen2", "https://accta.dfs.core.windows.net/fs/x", namespace=NS)
        b, _ = map_target(
            "AdlsGen2", "https://acctb.dfs.core.windows.net/fs/x", namespace=NS)
        self.assertNotEqual(a, b)
        self.assertEqual(a, "oci://accta_fs@acmens/x")
        self.assertEqual(b, "oci://acctb_fs@acmens/x")

    def test_two_blob_accounts_do_not_collide_on_one_bucket(self):
        a, _ = map_target(
            "AzureBlobStorage", "https://accta.blob.core.windows.net/fs/x",
            namespace=NS)
        b, _ = map_target(
            "AzureBlobStorage", "https://acctb.blob.core.windows.net/fs/x",
            namespace=NS)
        self.assertNotEqual(a, b)
        self.assertEqual(a, "oci://accta_fs@acmens/x")

    def test_blob_and_dfs_on_one_account_are_one_bucket(self):
        """Not a collision: multi-protocol access makes these the same bytes.

        `acct.blob...` and `acct.dfs...` are two endpoints onto one container
        in one account, so they must keep resolving to one OCI bucket. A fix
        that qualified by endpoint rather than by account would split them.
        """
        blob, _ = map_target(
            "AzureBlobStorage", "https://accta.blob.core.windows.net/fs/x",
            namespace=NS)
        dfs, _ = map_target(
            "AdlsGen2", "https://accta.dfs.core.windows.net/fs/x", namespace=NS)
        self.assertEqual(blob, dfs)

    def test_the_abfss_spelling_carries_the_account_too(self):
        a, _ = map_target(
            "AdlsGen2", "abfss://fs@accta.dfs.core.windows.net/x", namespace=NS)
        b, _ = map_target(
            "AdlsGen2", "abfss://fs@acctb.dfs.core.windows.net/x", namespace=NS)
        self.assertNotEqual(a, b)
        self.assertEqual(a, "oci://accta_fs@acmens/x")

    def test_the_abfss_and_https_spellings_agree(self):
        https, _ = map_target(
            "AdlsGen2", "https://accta.dfs.core.windows.net/fs/x", namespace=NS)
        abfss, _ = map_target(
            "AdlsGen2", "abfss://fs@accta.dfs.core.windows.net/x", namespace=NS)
        self.assertEqual(https, abfss)

    def test_an_adls_container_root_keeps_the_account(self):
        uri, _ = map_target(
            "AdlsGen2", "https://accta.dfs.core.windows.net/fs", namespace=NS)
        self.assertEqual(uri, "oci://accta_fs@acmens")

    def test_an_s3_bucket_name_is_global_so_it_stays_verbatim(self):
        """The aws-aidp convention is kept where it is sound.

        S3 bucket names are unique across all of AWS, so the bucket alone
        already identifies the data and nothing has to be prepended.
        """
        uri, _ = map_target(
            "AmazonS3", "https://acme.s3.us-east-1.amazonaws.com/claims",
            namespace=NS)
        self.assertEqual(uri, "oci://acme@acmens/claims")

    def test_a_gcs_bucket_name_is_global_so_it_stays_verbatim(self):
        uri, _ = map_target(
            "GoogleCloudStorage", "gs://gbucket/g", namespace=NS)
        self.assertEqual(uri, "oci://gbucket@acmens/g")


class S3CompatibleTests(unittest.TestCase):
    """`S3Compatible` is path-style, and its bucket is a field of its own.

    Microsoft documents `s3Compatible.location` as "HTTP URL of the S3
    compatible endpoint... The URL must be in the non-bucket specific format;
    no bucket should be specified here", alongside a sibling `bucket` field.
    Reading the first host label as the bucket -- which is what the
    virtual-host pattern did -- named the endpoint, not the data.
    """

    def test_the_bucket_is_the_first_path_segment_not_the_host_label(self):
        uri, reason = map_target(
            "S3Compatible", "https://s3.eu-west-1.amazonaws.com/data/claims",
            namespace=NS)
        self.assertIsNone(reason)
        self.assertIn("_data@", uri)
        self.assertNotIn("_s3@", uri)
        self.assertTrue(uri.endswith("/claims"), uri)

    def test_a_minio_endpoint_does_not_become_the_bucket(self):
        uri, _ = map_target(
            "S3Compatible", "https://minio.internal.corp/acme-prod/claims",
            namespace=NS)
        self.assertEqual(uri, "oci://minio.internal.corp_acme-prod@acmens/claims")

    def test_the_explicit_bucket_field_is_preferred_over_the_uri(self):
        """The documented Fabric shape: endpoint + bucket field + subpath."""
        uri, _ = map_target(
            "S3Compatible", "https://s3endpoint.contoso.com/data/Contoso",
            namespace=NS, bucket="contosoBucket1")
        self.assertEqual(
            uri, "oci://s3endpoint.contoso.com_contosoBucket1@acmens/data/Contoso")

    def test_two_endpoints_with_one_bucket_name_do_not_collide(self):
        a, _ = map_target("S3Compatible", "https://minio-a.corp/data/x",
                          namespace=NS)
        b, _ = map_target("S3Compatible", "https://minio-b.corp/data/x",
                          namespace=NS)
        self.assertNotEqual(a, b)

    def test_an_endpoint_with_no_bucket_anywhere_is_flagged_not_guessed(self):
        uri, reason = map_target(
            "S3Compatible", "https://s3endpoint.contoso.com", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("bucket", reason.lower())

    def test_a_scheme_form_bucket_contradicting_the_field_is_refused(self):
        uri, reason = map_target("S3Compatible", "s3://acme/x", namespace=NS,
                                 bucket="other")
        self.assertIsNone(uri)
        self.assertIn("acme", reason)
        self.assertIn("other", reason)

    def test_a_scheme_form_agreeing_with_the_field_still_maps(self):
        uri, reason = map_target("S3Compatible", "s3://acme/x", namespace=NS,
                                 bucket="acme")
        self.assertIsNone(reason)
        self.assertEqual(uri, "oci://acme@acmens/x")


class FabricEmittedShapeTests(unittest.TestCase):
    """One documented Fabric shape per target type, from Microsoft's own
    Create Shortcut examples and its per-type `location` format strings.

    A filed finding said "documented GCS (and other S3/ADLS host) shortcut
    shapes never parse". Measured: the two GCS forms the tests pinned --
    `gs://bkt/x` and `https://storage.googleapis.com/bkt/x` -- both work,
    and neither is a shape Fabric emits. Microsoft documents
    `googleCloudStorage.location` as "https://[bucket-name]
    .storage.googleapis.com", virtual-host style, and that is the one that
    did not parse. So the finding was right about the effect and wrong
    about which shapes: the tests pinned only non-Fabric forms.
    """

    # (target type, documented location + subpath, expected oci uri or None)
    DOCUMENTED = [
        ("AmazonS3",
         "https://my-s3-bucket.s3.us-west-2.amazonaws.com/data/ContosoEmployees",
         "oci://my-s3-bucket@acmens/data/ContosoEmployees"),
        ("AdlsGen2",
         "https://contosoadlsaccount.dfs.core.windows.net"
         "/mycontainer/data/ContosoProducts",
         "oci://contosoadlsaccount_mycontainer@acmens/data/ContosoProducts"),
        ("AzureBlobStorage",
         "https://azureblobstoragetesting.blob.core.windows.net/tables",
         "oci://azureblobstoragetesting_tables@acmens"),
        ("GoogleCloudStorage",
         "https://my-gcs-bucket.storage.googleapis.com/gcsDirectory/data",
         "oci://my-gcs-bucket@acmens/gcsDirectory/data"),
    ]

    def test_every_documented_object_store_shape_maps(self):
        for kind, uri, expected in self.DOCUMENTED:
            with self.subTest(kind=kind):
                got, reason = map_target(kind, uri, namespace=NS)
                self.assertIsNone(reason)
                self.assertEqual(got, expected)

    def test_the_fabric_gcs_form_is_virtual_host_style(self):
        """The shape the finding was really about."""
        uri, reason = map_target(
            "GoogleCloudStorage",
            "https://gcs-contosobucket.storage.googleapis.com"
            "/gcsDirectory/data/ContosoProducts", namespace=NS)
        self.assertIsNone(reason)
        self.assertEqual(uri, "oci://gcs-contosobucket@acmens"
                              "/gcsDirectory/data/ContosoProducts")

    def test_a_gcs_bucket_root_maps(self):
        uri, _ = map_target("GoogleCloudStorage",
                            "https://my-gcs-bucket.storage.googleapis.com",
                            namespace=NS)
        self.assertEqual(uri, "oci://my-gcs-bucket@acmens")

    def test_the_path_style_gcs_form_still_means_what_it_did(self):
        """Adding the virtual-host form must not make `storage` a bucket."""
        uri, _ = map_target("GoogleCloudStorage",
                            "https://storage.googleapis.com/bkt/x", namespace=NS)
        self.assertEqual(uri, "oci://bkt@acmens/x")

    def test_the_gs_scheme_form_still_works(self):
        uri, _ = map_target("GoogleCloudStorage", "gs://gbucket/g", namespace=NS)
        self.assertEqual(uri, "oci://gbucket@acmens/g")

    def test_dataverse_is_refused_as_not_object_storage(self):
        uri, reason = map_target(
            "Dataverse", "https://orgname.crm11.dynamics.com/accounts",
            namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("not object storage", reason)

    def test_onedrive_sharepoint_is_refused_as_not_object_storage(self):
        uri, reason = map_target(
            "OneDriveSharePoint",
            "https://microsoft.sharepoint.com/Shared Documents", namespace=NS)
        self.assertIsNone(uri)
        self.assertIn("not object storage", reason)


class TranslateTests(unittest.TestCase):
    def test_mapped_shortcut_has_no_flags(self):
        result = translate(_shortcut(), namespace=NS)
        self.assertEqual(result.flags, 0)
        self.assertEqual(result.changes, 1)

    def test_artifact_names_both_locations(self):
        out = translate(_shortcut(), namespace=NS).translated_sql
        self.assertIn("https://acme.s3.us-east-1.amazonaws.com/claims", out)
        self.assertIn("oci://acme@acmens/claims", out)
        self.assertIn("claims_raw", out)

    def test_artifact_says_data_movement_is_separate(self):
        out = translate(_shortcut(), namespace=NS).translated_sql
        self.assertIn("does not copy", out.lower())

    def test_unmappable_shortcut_is_flagged_and_still_produces_an_artifact(self):
        result = translate(_shortcut(target_type="Dataverse"), namespace=NS)
        self.assertEqual(result.flags, 1)
        self.assertIn("claims_raw", result.translated_sql)

    def test_internal_shortcut_is_flagged(self):
        result = translate(
            _shortcut(target_type="OneLake", external=False,
                      target="onelake://ws/item/Tables/t"), namespace=NS)
        self.assertEqual(result.flags, 1)

    def test_files_section_shortcut_still_maps(self):
        """The location is real whichever section it is in, so the mapping
        still happens. What changed is that it is no longer silent about
        the section -- see FilesSectionTests."""
        result = translate(_shortcut(section="Files"), namespace=NS)
        self.assertEqual([f.rule for f in result.findings
                          if f.severity == "rewrite"],
                         ["SC10_SHORTCUT_TARGET"])

    def test_an_s3_compatible_bucket_field_reaches_the_mapping(self):
        result = translate(_shortcut(
            name="minio_claims", target_type="S3Compatible",
            target="https://s3endpoint.contoso.com/data/Contoso",
            bucket="contosoBucket1"), namespace=NS)
        self.assertEqual(result.flags, 0)
        self.assertIn("oci://s3endpoint.contoso.com_contosoBucket1@acmens"
                      "/data/Contoso", result.translated_sql)

    def test_the_note_records_the_bucket_the_uri_does_not_carry(self):
        """The endpoint URL is non-bucket-specific, so printing the source
        line alone would hide which bucket the data is actually in."""
        out = translate(_shortcut(
            name="minio_claims", target_type="S3Compatible",
            target="https://s3endpoint.contoso.com/data/Contoso",
            bucket="contosoBucket1"), namespace=NS).translated_sql
        self.assertIn("contosoBucket1", out.split("AIDP target")[0])



class FilesSectionTests(unittest.TestCase):
    """A `Files` shortcut is a folder of objects. There is no table there.

    The plan gave it the target type `aidp_dcat_external_table` and the
    note told the reader to "register the location as an AIDP external
    table", so both documents described an object the migration cannot
    produce and AIDP has nothing to register. The data location is real
    and still mapped; what is handed back, named, is the decision that has
    no automatic answer.
    """

    def test_a_files_shortcut_is_flagged_as_not_a_table(self):
        result = translate(_shortcut(section="Files"), namespace=NS)
        self.assertIn("SC12_NOT_A_TABLE", [f.rule for f in result.findings])

    def test_the_finding_says_why_and_what_is_left_to_do(self):
        finding = next(f for f in translate(_shortcut(section="Files"),
                                            namespace=NS).findings
                       if f.rule == "SC12_NOT_A_TABLE")
        self.assertEqual(finding.severity, "flag")
        self.assertIn("folder", finding.detail)
        self.assertIn("Files", finding.detail)

    def test_a_tables_shortcut_is_not_flagged(self):
        result = translate(_shortcut(section="Tables"), namespace=NS)
        self.assertNotIn("SC12_NOT_A_TABLE", [f.rule for f in result.findings])

    def test_the_note_stops_telling_the_reader_to_register_a_table(self):
        out = translate(_shortcut(section="Files"), namespace=NS).translated_sql
        self.assertNotIn("external table", out.lower())
        self.assertIn("not a table", out.lower())

    def test_the_note_still_says_it_for_a_tables_shortcut(self):
        out = translate(_shortcut(section="Tables"), namespace=NS).translated_sql
        self.assertIn("external table", out.lower())

    def test_the_note_prints_the_path_when_it_says_more_than_the_section(self):
        """`Tables` and `Tables/dbo` put the same name in two places."""
        out = translate(_shortcut(section="Tables", path="Tables/dbo"),
                        namespace=NS).translated_sql
        self.assertIn("Tables/dbo", out)


class ShapeProvenanceTests(unittest.TestCase):
    """Which shortcut shapes are measured and which are read.

    `_EXTERNAL_TARGETS` holds seven external target shapes and treated them
    all alike, so nothing said which field names had been seen in a real
    Fabric export and which came off Microsoft's documentation and have
    never been observed. That distinction is the difference between a
    regression and a guess: if `dataverse.environmentDomain` is not what
    Fabric actually writes, no test here can tell, because every input that
    exercises it was written from the same page.

    MEASURED 2026-09-29 across everything vendored in this repository:

        oneLake             in two real exports under tests/fixtures/real/
        amazonS3, adlsGen2  in the demo-workspace fixture this project
                            authored, from the same documentation
        googleCloudStorage, azureBlobStorage, s3Compatible, dataverse,
        oneDriveSharePoint  nowhere but the table itself and tests beside it

    Closing the gap needs a live-tenant export with an ADLS/GCS/S3-compatible
    shortcut in it, which cannot be written here. What can be done, and is
    what this class enforces, is that the claim beside each shape stays true:
    a row marked as seen in a real export has to be findable in one, and a
    row marked documentation-only must not quietly become fixture-backed
    without the note being updated.
    """

    REAL_FIXTURES = ROOT / "tests" / "fixtures" / "real"

    @classmethod
    def setUpClass(cls):
        cls.in_real_exports = set()
        for path in cls.REAL_FIXTURES.rglob("shortcuts.metadata.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            for entry in payload if isinstance(payload, list) else []:
                target = entry.get("target") if isinstance(entry, dict) else None
                for key in target or {}:
                    cls.in_real_exports.add(key.casefold())
        cls.in_real_exports.discard("type")

    def test_every_shape_the_scanner_knows_has_a_provenance(self):
        known = set(_EXTERNAL_TARGETS) | {"onelake"}
        self.assertEqual(sorted(known), sorted(SHAPE_PROVENANCE),
                         "a shortcut shape was added or removed without "
                         "saying where it came from")

    def test_a_shape_claimed_from_a_real_export_is_in_one(self):
        for shape, origin in sorted(SHAPE_PROVENANCE.items()):
            if origin != FROM_REAL_EXPORT:
                continue
            with self.subTest(shape=shape):
                self.assertIn(
                    shape, self.in_real_exports,
                    f"{shape} is documented as seen in a real export and no "
                    f"shortcuts.metadata.json under {self.REAL_FIXTURES} "
                    f"contains it")

    def test_a_shape_not_so_claimed_is_not_in_one_either(self):
        """The other direction, which is the one that goes stale. Vendor a
        real export carrying an ADLS shortcut and this fails, which is the
        prompt to upgrade the note from a reading to a measurement."""
        for shape, origin in sorted(SHAPE_PROVENANCE.items()):
            if origin == FROM_REAL_EXPORT:
                continue
            with self.subTest(shape=shape):
                self.assertNotIn(
                    shape, self.in_real_exports,
                    f"{shape} now appears in a vendored real export, so it "
                    f"is no longer {origin!r}. Mark it FROM_REAL_EXPORT.")

    def test_only_one_shape_is_backed_by_a_real_export(self):
        """The headline number, stated so it cannot drift silently: seven of
        the eight shapes this tool recognises have never been seen."""
        measured = [s for s, o in SHAPE_PROVENANCE.items() if o == FROM_REAL_EXPORT]
        self.assertEqual(measured, ["onelake"])
        self.assertEqual(
            sum(1 for o in SHAPE_PROVENANCE.values() if o == DOCUMENTED_ONLY), 5)

    def test_the_authored_fixture_really_does_carry_the_two_it_claims(self):
        """The weaker claim, checked too: `amazonS3` and `adlsGen2` are not
        measured, but the demo estate does exercise them, and a note saying
        so has to be true as well."""
        demo = json.loads((ROOT / "fabric_aidp" / "fixtures" / "demo-workspace"
                           / "SalesLake.Lakehouse" / "shortcuts.metadata.json"
                           ).read_text(encoding="utf-8"))
        present = {key.casefold() for entry in demo
                   for key in (entry.get("target") or {})} - {"type"}
        for shape, origin in sorted(SHAPE_PROVENANCE.items()):
            if origin == FROM_AUTHORED_FIXTURE:
                with self.subTest(shape=shape):
                    self.assertIn(shape, present)


if __name__ == "__main__":
    unittest.main()
