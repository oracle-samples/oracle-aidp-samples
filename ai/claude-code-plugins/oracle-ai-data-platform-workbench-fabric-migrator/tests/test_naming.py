import unittest

from fabric_aidp.naming import (
    DEFAULT_CATALOG, NamingError, addressable, aidp_schema, aidp_table,
    quote_part, spark_column_ref, unaddressable_parts, unaddressable_schema,
    unaddressable_schema_detail, validated_catalog,
)


class SchemaTests(unittest.TestCase):
    def test_fabrics_default_schema_is_dropped(self):
        self.assertEqual(aidp_schema("AcmeDW", "dbo"), "AcmeDW")

    def test_dropping_is_case_insensitive(self):
        self.assertEqual(aidp_schema("AcmeDW", "DBO"), "AcmeDW")

    def test_any_other_schema_is_kept_as_a_suffix(self):
        self.assertEqual(aidp_schema("AcmeDW", "postgres_air"),
                         "AcmeDW_postgres_air")

    def test_an_item_with_no_schema_is_itself(self):
        self.assertEqual(aidp_schema("SalesLake"), "SalesLake")

    def test_brackets_and_backticks_are_stripped(self):
        self.assertEqual(aidp_schema("[SalesLake]", "[dbo]"), "SalesLake")

    def test_schema_without_an_item_raises(self):
        """Cannot occur in a Fabric export; the alternative is emitting
        `<catalog>._<schema>.<table>`, which looks deliberate and is not."""
        with self.assertRaises(NamingError):
            aidp_schema("", "sales")


class TableTests(unittest.TestCase):
    def test_lakehouse_table(self):
        self.assertEqual(aidp_table("SalesLake", "claim"),
                         "default.SalesLake.claim")

    def test_warehouse_table_loses_dbo(self):
        self.assertEqual(aidp_table("AcmeDW", "claim", schema="dbo"),
                         "default.AcmeDW.claim")

    def test_warehouse_table_keeps_a_real_schema(self):
        self.assertEqual(aidp_table("AcmeDW", "t", schema="postgres_air"),
                         "default.AcmeDW_postgres_air.t")

    def test_schema_enabled_lakehouse(self):
        self.assertEqual(aidp_table("SalesLake", "claim", schema="sales"),
                         "default.SalesLake_sales.claim")

    def test_no_item_yields_two_parts_not_a_fabricated_schema(self):
        """NB11 already flags a missing lakehouse. Inventing a placeholder
        schema would turn a flagged unknown into a confident wrong name."""
        self.assertEqual(aidp_table("", "claim"), "default.claim")

    def test_the_catalog_is_configurable(self):
        self.assertEqual(aidp_table("SalesLake", "claim", catalog="myc"),
                         "myc.SalesLake.claim")

    def test_an_empty_catalog_falls_back_to_the_default(self):
        self.assertEqual(aidp_table("SalesLake", "claim", catalog=""),
                         "default.SalesLake.claim")

    def test_a_missing_table_raises(self):
        with self.assertRaises(NamingError):
            aidp_table("SalesLake", "")

    def test_the_default_catalog_is_what_the_constant_says(self):
        self.assertTrue(aidp_table("L", "t").startswith(DEFAULT_CATALOG + "."))


class QuotingTests(unittest.TestCase):
    """The helper stripped every quote off its inputs and put none back, so a
    real corpus item name came out as an expression. `on-prem-wh` unquoted is
    a subtraction Spark is happy to evaluate."""

    def test_a_plain_identifier_is_left_bare(self):
        self.assertEqual(quote_part("SalesLake"), "SalesLake")

    def test_a_hyphen_is_quoted(self):
        self.assertEqual(
            aidp_table("fabric-data-engineering-ws_on-prem-warehouse-test-wh",
                       "synthetic_orders"),
            "default.`fabric-data-engineering-ws_on-prem-warehouse-test-wh`"
            ".synthetic_orders")

    def test_a_space_is_quoted(self):
        self.assertEqual(aidp_table("Sales Lake", "claim"),
                         "default.`Sales Lake`.claim")

    def test_a_leading_digit_is_quoted(self):
        self.assertEqual(aidp_table("LH", "2024_orders"),
                         "default.LH.`2024_orders`")

    def test_a_suffixed_schema_is_quoted_as_one_part(self):
        self.assertEqual(aidp_table("Sales Lake", "claim", schema="sales"),
                         "default.`Sales Lake_sales`.claim")

    def test_an_embedded_backtick_is_doubled(self):
        self.assertEqual(quote_part("we`ird"), "`we``ird`")

    def test_the_common_name_is_unchanged_by_quoting(self):
        """Quoting every part would be safe and unreadable; the point of
        quoting only what needs it is that the ordinary name does not move."""
        self.assertEqual(aidp_table("SalesLake", "claim"),
                         "default.SalesLake.claim")


class ValidatedCatalogTests(unittest.TestCase):
    """`--namespace` has been validated since the first release; `--catalog`
    reached the first position of every table name unchecked."""

    def test_a_plain_catalog_passes_through(self):
        # A single lowercase token, the shape a real AIDP catalog takes. It
        # used to be a tenant's actual catalog name; the shape is what the
        # test is about, so the name is now a neutral one.
        self.assertEqual(validated_catalog("lakehouse_gold"), "lakehouse_gold")

    def test_a_hyphen_is_allowed(self):
        self.assertEqual(validated_catalog("my-cat"), "my-cat")

    def test_empty_means_the_default(self):
        self.assertEqual(validated_catalog(""), DEFAULT_CATALOG)
        self.assertEqual(validated_catalog(None), DEFAULT_CATALOG)

    def test_a_dot_is_refused_because_it_adds_a_name_part(self):
        with self.assertRaises(NamingError):
            validated_catalog("a.b")

    def test_a_space_is_refused(self):
        with self.assertRaises(NamingError):
            validated_catalog("my catalog")

    def test_a_backtick_is_refused(self):
        with self.assertRaises(NamingError):
            validated_catalog("a`b")

    def test_a_leading_digit_is_refused(self):
        with self.assertRaises(NamingError):
            validated_catalog("1cat")


class SparkColumnRefTests(unittest.TestCase):
    """A column reference is parsed; a column *name* is not.

    MEASURED on pyspark 4.2.0, a frame with a flat column literally called
    `a.b`:

        df.select("a.b")     FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
        F.col("a.b")         FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
        df["a.b"]            FAIL UNRESOLVED_COLUMN.WITH_SUGGESTION
        F.col("`a.b`")       OK   ['a.b']
        df.select("`a.b`")   OK   ['a.b']
        df.drop("a.b")       OK   ['c']        <- a name, never parsed

    Power Query produces dotted column names by default:
    `Table.ExpandRecordColumn` names its output `Customer.Name`.
    """

    def test_a_dot_is_quoted_because_it_is_the_identifier_separator(self):
        self.assertEqual(spark_column_ref("Customer.Name"), "`Customer.Name`")

    def test_a_backtick_in_the_name_is_doubled(self):
        # Measured: F.col("`a``b`") resolves the column named a`b.
        self.assertEqual(spark_column_ref("a`b"), "`a``b`")

    def test_a_hyphen_and_a_space_are_left_alone(self):
        """The half of the filed finding that is refuted. Measured:
        df.select("order-id"), F.col("order-id") and F.col("line total")
        all resolve with no quoting at all, so backticking them would
        rewrite every generated file in this repo to no effect."""
        self.assertEqual(spark_column_ref("order-id"), "order-id")
        self.assertEqual(spark_column_ref("line total"), "line total")
        self.assertEqual(spark_column_ref("plain"), "plain")


class UnaddressablePartsTests(unittest.TestCase):
    """Which parts of an `aidp_table` name a catalog rejects.

    The alphabet is AIDP's, measured on the cluster; see
    AidpSchemaAlphabetTests. The same shapes were first MEASURED on pyspark 4.2.0, built-in Hive-compatible catalog:
    saveAsTable("pdb.plain") succeeds; `my-table`, `my table`, `my.table`
    and a non-ASCII letter all fail INVALID_SCHEMA_OR_RELATION_NAME even
    back-quoted, and so does CREATE DATABASE `my-lake`.
    """

    def test_a_plain_name_has_no_unaddressable_part(self):
        self.assertEqual(unaddressable_parts(aidp_table("SalesLake", "claim")), [])
        self.assertEqual(unaddressable_parts(aidp_table("", "claim")), [])

    def test_a_hyphenated_table_is_named(self):
        self.assertEqual(
            unaddressable_parts(aidp_table("SalesLake", "my-table")),
            ["my-table"])

    def test_every_offending_part_is_named_not_just_the_first(self):
        self.assertEqual(
            unaddressable_parts(aidp_table("Sales Lake", "my table")),
            ["Sales Lake", "my table"])

    def test_the_back_quoting_in_the_name_is_read_through(self):
        # The input is what aidp_table built, backticks and all, so the
        # check has to unwrap them rather than see `\`my-table\`` as the part.
        self.assertEqual(
            unaddressable_parts("default.SalesLake.`my-table`"), ["my-table"])


class AidpSchemaAlphabetTests(unittest.TestCase):
    """The schema-name alphabet AIDP's metastore enforces.

    MEASURED on an AIDP cluster (Spark 3.5.0), 2026-09-30:
    `CREATE SCHEMA IF NOT EXISTS default.\`fabric-data-engineering-ws_on-prem-
    warehouse-test-wh\`` -> MetaException(... Only lower-case characters,
    numbers and underscores are allowed.); the same for `R2 Sales Lake` and
    `r2Ünï`. `default.R2MixedCase` succeeded and SHOW SCHEMAS listed
    `r2mixedcase`. So: [a-z0-9_] after case-folding.
    """

    def test_the_three_names_the_cluster_refused_are_unaddressable(self):
        for name in ("fabric-data-engineering-ws_on-prem-warehouse-test-wh",
                     "R2 Sales Lake", "r2Ünï"):
            with self.subTest(name=name):
                self.assertFalse(addressable(name))
                self.assertEqual(unaddressable_schema(name), name)

    def test_mixed_case_is_addressable_because_the_metastore_folds_it(self):
        self.assertTrue(addressable("R2MixedCase"))
        self.assertEqual(unaddressable_schema("SalesLake"), "")
        self.assertEqual(unaddressable_schema("AcmeDW", "sales"), "")

    def test_a_kelvin_sign_is_not_folded_into_an_ascii_k(self):
        # "\u212a".lower() == "k"; the metastore was never shown one.
        self.assertFalse(addressable("\u212aelvin"))

    def test_the_check_is_on_the_joined_schema_not_on_the_item_alone(self):
        self.assertEqual(unaddressable_schema("AcmeDW", "my-schema"),
                         "AcmeDW_my-schema")
        self.assertEqual(unaddressable_schema("my-lake", "dbo"), "my-lake")

    def test_no_item_means_no_schema_part_to_refuse(self):
        self.assertEqual(unaddressable_schema(""), "")

    def test_the_finding_names_the_schema_and_the_measurement(self):
        detail = unaddressable_schema_detail(["my-lake"])
        self.assertIn("'my-lake'", detail)
        self.assertIn("cannot be created on AIDP", detail)
        self.assertIn("2026-09-30", detail)


if __name__ == "__main__":
    unittest.main()
