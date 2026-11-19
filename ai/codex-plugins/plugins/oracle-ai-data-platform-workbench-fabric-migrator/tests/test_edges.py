import unittest

from fabric_aidp.inventory.edges import (
    extract_notebook_edges, first_argument, string_literal_value,
)


class StringLiteralTests(unittest.TestCase):
    def test_recognises_both_quote_styles(self):
        self.assertEqual(string_literal_value(' "Common_Utils" '), "Common_Utils")
        self.assertEqual(string_literal_value("'Common_Utils'"), "Common_Utils")

    def test_rejects_a_variable(self):
        self.assertIsNone(string_literal_value("nb_name"))

    def test_rejects_an_fstring(self):
        self.assertIsNone(string_literal_value('f"nb_{env}"'))


class FirstArgumentTests(unittest.TestCase):
    def test_stops_at_the_first_top_level_comma(self):
        text = 'run("A", 60, {"k": "v"})'
        self.assertEqual(first_argument(text, text.index("(")), '"A"')

    def test_ignores_commas_inside_nested_calls(self):
        text = 'run(pick(a, b), 60)'
        self.assertEqual(first_argument(text, text.index("(")), "pick(a, b)")

    def test_ignores_commas_inside_strings(self):
        text = 'run("A,B")'
        self.assertEqual(first_argument(text, text.index("(")), '"A,B"')

    def test_unterminated_call_returns_none(self):
        text = 'run("A"'
        self.assertIsNone(first_argument(text, text.index("(")))


class RunMagicTests(unittest.TestCase):
    def test_bare_run_magic(self):
        e = extract_notebook_edges("%run Common_Utils\n")
        self.assertEqual(e["run"], ["Common_Utils"])

    def test_quoted_run_magic(self):
        self.assertEqual(extract_notebook_edges('%run "Common Utils"\n')["run"],
                         ["Common Utils"])

    def test_run_magic_with_trailing_parameters(self):
        e = extract_notebook_edges('%run Common_Utils {"env": "prod"}\n')
        self.assertEqual(e["run"], ["Common_Utils"])

    def test_commented_run_magic_is_ignored(self):
        self.assertEqual(extract_notebook_edges("# %run Commented\n")["run"], [])

    def test_indented_run_magic_is_found(self):
        self.assertEqual(extract_notebook_edges("    %run Indented\n")["run"], ["Indented"])


class NotebookRunTests(unittest.TestCase):
    def test_notebookutils_literal_target(self):
        src = 'notebookutils.notebook.run("Refresh_Dims", 300, {"d": "1"})'
        self.assertEqual(extract_notebook_edges(src)["notebook_run"], ["Refresh_Dims"])

    def test_mssparkutils_alias_is_recognised(self):
        src = 'mssparkutils.notebook.run("Legacy_Nb")'
        self.assertEqual(extract_notebook_edges(src)["notebook_run"], ["Legacy_Nb"])

    def test_variable_target_is_unresolved_not_dropped(self):
        src = "notebookutils.notebook.run(nb_name, 300)"
        e = extract_notebook_edges(src)
        self.assertEqual(e["notebook_run"], [])
        self.assertEqual(e["unresolved"], ["nb_name"])

    def test_fstring_target_is_unresolved(self):
        src = 'notebookutils.notebook.run(f"ingest_{env}")'
        self.assertEqual(extract_notebook_edges(src)["unresolved"], ['f"ingest_{env}"'])


class AggregationTests(unittest.TestCase):
    def test_results_are_deduplicated_and_sorted(self):
        src = "%run Zeta\n%run Alpha\n%run Zeta\n"
        self.assertEqual(extract_notebook_edges(src)["run"], ["Alpha", "Zeta"])

    def test_empty_source_yields_empty_lists(self):
        self.assertEqual(extract_notebook_edges(""),
                         {"run": [], "notebook_run": [], "run_multiple": [],
                          "unresolved": []})

    def test_non_string_input_yields_empty_lists(self):
        self.assertEqual(extract_notebook_edges(None)["run"], [])



class RunMagicFlagTests(unittest.TestCase):
    """`%run -b Child` yielded `{'run': ['-b']}`.

    The flag was read as the notebook name, so the real edge to `Child`
    was lost *and* a false one to a notebook called `-b` was invented:
    end to end the plan reported
    `warnings.dangling_depends_on == ["notebook.-b"]` and
    `notebook.Parent_b_flag depends_on= []`. One line of source turned a
    true edge into a false one, which is worse than missing it.
    """

    def test_a_leading_flag_is_not_the_notebook_name(self):
        self.assertEqual(extract_notebook_edges("%run -b Child")["run"],
                         ["Child"])

    def test_a_long_flag_is_not_either(self):
        self.assertEqual(extract_notebook_edges("%run --builtin Child")["run"],
                         ["Child"])

    def test_several_flags_are_all_skipped(self):
        self.assertEqual(
            extract_notebook_edges("%run -b -c Child")["run"], ["Child"])

    def test_a_flag_before_a_quoted_name_still_finds_it(self):
        self.assertEqual(
            extract_notebook_edges('%run -b "My Notebook"')["run"],
            ["My Notebook"])

    def test_a_flag_before_a_name_and_parameters(self):
        self.assertEqual(
            extract_notebook_edges('%run -b Child { "p": 1 }')["run"],
            ["Child"])

    def test_a_line_that_is_only_flags_names_nothing(self):
        self.assertEqual(extract_notebook_edges("%run -b")["run"], [])

    def test_the_unflagged_form_is_unchanged(self):
        self.assertEqual(extract_notebook_edges("%run Child")["run"], ["Child"])


class RunMultipleTests(unittest.TestCase):
    """`runMultiple` produced silence: no edge and no warning.

    `_NB_RUN_RE` matches `.run\\s*\\(`, which does not match
    `.runMultiple(`, so a notebook that launched ten children with one
    call contributed nothing to the plan. Both documented argument shapes
    are read, with `ast.literal_eval`, so nothing is executed to find out.
    """

    def test_a_list_of_names(self):
        self.assertEqual(
            extract_notebook_edges(
                'notebookutils.notebook.runMultiple(["Child", "Other"])'
            )["run_multiple"], ["Child", "Other"])

    def test_the_dag_form_reads_the_path(self):
        source = ('notebookutils.notebook.runMultiple({"activities": ['
                  '{"name": "step one", "path": "Child", "dependencies": []}]})')
        self.assertEqual(extract_notebook_edges(source)["run_multiple"],
                         ["Child"])

    def test_the_dag_form_falls_back_to_the_activity_name(self):
        source = ('notebookutils.notebook.runMultiple('
                  '{"activities": [{"name": "Child"}]})')
        self.assertEqual(extract_notebook_edges(source)["run_multiple"],
                         ["Child"])

    def test_the_mssparkutils_spelling_works_too(self):
        self.assertEqual(
            extract_notebook_edges(
                'mssparkutils.notebook.runMultiple(["Child"])'
            )["run_multiple"], ["Child"])

    def test_a_variable_argument_is_unresolved_rather_than_silent(self):
        got = extract_notebook_edges("notebookutils.notebook.runMultiple(dag)")
        self.assertEqual(got["run_multiple"], [])
        self.assertEqual(got["unresolved"], ["dag"])

    def test_a_literal_of_an_unknown_shape_is_unresolved_too(self):
        """Saying "no notebooks" would be a claim; this is the truth."""
        got = extract_notebook_edges(
            'notebookutils.notebook.runMultiple({"steps": ["Child"]})')
        self.assertEqual(got["unresolved"],
                         ['{"steps": ["Child"]}'])

    def test_an_empty_list_is_read_and_names_nobody(self):
        got = extract_notebook_edges("notebookutils.notebook.runMultiple([])")
        self.assertEqual((got["run_multiple"], got["unresolved"]), ([], []))

    def test_run_and_run_multiple_do_not_shadow_each_other(self):
        source = ('notebookutils.notebook.run("One")\n'
                  'notebookutils.notebook.runMultiple(["Two"])\n')
        got = extract_notebook_edges(source)
        self.assertEqual(got["notebook_run"], ["One"])
        self.assertEqual(got["run_multiple"], ["Two"])


if __name__ == "__main__":
    unittest.main()
