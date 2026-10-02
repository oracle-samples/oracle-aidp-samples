import json
import unittest

from fabric_aidp.translate import pipeline_to_aidp_job as p2j

PATHS = {"01_Ingest": "/Workspace/01_Ingest.py", "02_Agg": "/Workspace/02_Agg.py"}


def _nb(name, notebook, depends=()):
    return {"name": name, "type": "TridentNotebook", "notebook": notebook,
            "pipeline": "", "depends_on": list(depends), "parameters": {}}


def _run(activities, **kw):
    kw.setdefault("notebook_paths", PATHS)
    return p2j.translate({"name": "Daily", "activities": activities}, **kw)


def _payload(result):
    return json.loads(result.translated_sql)


class TaskKeyTests(unittest.TestCase):
    def test_spaces_become_underscores(self):
        self.assertEqual(p2j.task_key("Load Meters"), "Load_Meters")

    def test_punctuation_is_replaced(self):
        self.assertEqual(p2j.task_key("Load/Meters (v2)"), "Load_Meters_v2")

    def test_an_empty_name_still_yields_a_key(self):
        self.assertEqual(p2j.task_key(""), "task")

    def test_duplicate_names_do_not_collide(self):
        payload = _payload(_run([_nb("Run", "01_Ingest"), _nb("Run", "02_Agg")]))
        self.assertEqual([t["taskKey"] for t in payload["tasks"]], ["Run", "Run_2"])

    def test_a_suffix_never_takes_a_key_another_activity_owns(self):
        # PL_KeyCollide, from the live probe workspace. The per-base counter
        # gave Load_A, Load_A_2, Load_A_2: the third task depended on itself
        # and Final's upstream was ambiguous.
        acts = [_nb("Load A", "01_Ingest"),
                _nb("Load_A", "02_Agg", ["Load A"]),
                _nb("Load_A_2", "01_Ingest", ["Load_A"]),
                _nb("Final", "02_Agg", ["Load_A_2"])]
        tasks = _payload(_run(acts))["tasks"]
        self.assertEqual([t["taskKey"] for t in tasks],
                         ["Load_A", "Load_A_3", "Load_A_2", "Final"])
        self.assertEqual([t.get("dependsOn") for t in tasks], [
            None, [{"taskKey": "Load_A"}], [{"taskKey": "Load_A_3"}],
            [{"taskKey": "Load_A_2"}]])

    def test_the_repro_pipeline_end_to_end(self):
        # The exported PL_KeyCollide item, through inventory and translation.
        import tempfile
        from pathlib import Path
        from fabric_aidp.inventory import pipeline as inventory
        from fabric_aidp.inventory.git_workspace import discover_items
        body = {"properties": {"activities": [
            {"name": "Load A", "type": "TridentNotebook", "dependsOn": [],
             "typeProperties": {"notebookName": "NB_Load"}},
            {"name": "Load_A", "type": "TridentNotebook",
             "dependsOn": [{"activity": "Load A", "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookName": "NB_Other"}},
            {"name": "Load_A_2", "type": "TridentNotebook",
             "dependsOn": [{"activity": "Load_A", "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookName": "NB_Alert"}},
            {"name": "Final", "type": "TridentNotebook",
             "dependsOn": [{"activity": "Load_A_2", "dependencyConditions": ["Succeeded"]}],
             "typeProperties": {"notebookName": "NB Transform"}}]}}
        with tempfile.TemporaryDirectory() as tmp:
            item = Path(tmp) / "PL_KeyCollide.DataPipeline"
            item.mkdir()
            (item / ".platform").write_text(json.dumps({
                "version": "2.0", "config": {"logicalId": "00e8150d-5907-4c30-bfb3-fd70ea3bba58"},
                "metadata": {"type": "DataPipeline", "displayName": "PL_KeyCollide"}}))
            (item / "pipeline-content.json").write_text(json.dumps(body))
            record = inventory.scan(discover_items(Path(tmp)))["items"]["pipelines"][0]
        paths = {n: f"/Workspace/{n}.ipynb" for n in record["notebook_refs"]}
        tasks = _payload(p2j.translate(record, notebook_paths=paths))["tasks"]
        by_key = {t["taskKey"]: t for t in tasks}
        self.assertEqual(len(by_key), 4)
        self.assertEqual(by_key["Final"]["dependsOn"], [{"taskKey": "Load_A_2"}])
        self.assertEqual(by_key["Load_A_2"]["notebookPath"], "/Workspace/NB_Alert.ipynb")
        for task in tasks:
            self.assertNotIn({"taskKey": task["taskKey"]}, task.get("dependsOn", []))

    def test_keys_are_unique_for_any_mix_of_colliding_names(self):
        import itertools
        names = ["a", "a_2", "a 2", "a-2", "a_2_2", "a_3", "a"]
        for combo in itertools.permutations(names, 5):
            acts = [_nb(n, "01_Ingest") for n in combo]
            keys = [t["taskKey"] for t in _payload(_run(acts))["tasks"]]
            with self.subTest(names=combo):
                self.assertEqual(len(keys), len(set(keys)))
                self.assertEqual(keys, [t["taskKey"] for t in _payload(_run(acts))["tasks"]])


class JobNameTests(unittest.TestCase):
    def test_a_display_name_becomes_a_name_aidp_accepts(self):
        # AIDP create-job: 400 "Must start with letter and no special characters".
        self.assertEqual(p2j.job_name("Daily Refresh"), "Daily_Refresh")
        self.assertEqual(p2j.job_name("Nightly Load-EU.v2"), "Nightly_Load_EU_v2")
        self.assertEqual(p2j.job_name("1st load"), "job_1st_load")
        self.assertEqual(p2j.job_name(""), "job")

    def test_the_job_carries_the_clean_name_and_says_so(self):
        result = p2j.translate({"name": "Daily Refresh", "activities": [
            _nb("Ingest", "01_Ingest")]}, notebook_paths=PATHS)
        self.assertEqual(_payload(result)["name"], "Daily_Refresh")
        self.assertIn("PL14_JOB_NAME", [f.rule for f in result.findings])


CRON = {"enabled": True, "job_type": "Pipeline", "configuration": {
    "type": "Cron", "startDateTime": "2026-01-01T02:00:00", "interval": 60,
    "endDateTime": "2027-01-01T00:00:00", "localTimeZoneId": "UTC"}}


class ScheduleTests(unittest.TestCase):
    """PL_Params runs every 60 minutes in Fabric; its job said nothing."""

    def _run(self, **pipeline):
        return p2j.translate({"name": "Daily", "activities": [_nb("Ingest", "01_Ingest")],
                              **pipeline}, notebook_paths=PATHS, cluster_key="c")

    def _schedule_findings(self, result):
        return [f for f in result.findings if f.rule == "PL20_SCHEDULE"]

    def test_an_enabled_schedule_is_flagged_with_its_settings(self):
        result = self._run(schedules=[CRON])
        (finding,) = self._schedule_findings(result)
        self.assertEqual(finding.severity, "flag")
        for part in ("Cron schedule", "every 60 min", "time zone UTC",
                     "from 2026-01-01T02:00:00", "until 2027-01-01T00:00:00",
                     "recreate the schedule"):
            self.assertIn(part, finding.detail)
        self.assertTrue(result.needs_manual_review)

    def test_the_job_is_still_emitted_and_carries_no_schedule(self):
        payload = _payload(self._run(schedules=[CRON]))
        self.assertNotIn("schedule", payload)
        self.assertEqual(len(payload["tasks"]), 1)

    def test_weekly_and_unknown_fields_are_described(self):
        weekly = {"enabled": True, "configuration": {
            "type": "Weekly", "weekdays": ["Monday", "Friday"], "times": ["06:30"],
            "localTimeZoneId": "India Standard Time", "recurrence": 2}}
        (finding,) = self._schedule_findings(self._run(schedules=[weekly]))
        for part in ("Weekly schedule", "at 06:30", "on Monday, Friday",
                     "time zone India Standard Time", "recurrence=2"):
            self.assertIn(part, finding.detail)

    def test_both_spellings_of_weekdays_are_rendered_once(self):
        # Both spellings were fields of their own, so a configuration
        # carrying both read "on Monday, on Monday".
        weekly = {"enabled": True, "configuration": {
            "type": "Weekly", "weekdays": ["Monday"], "weekDays": ["Monday"]}}
        (finding,) = self._schedule_findings(self._run(schedules=[weekly]))
        self.assertEqual(finding.detail.count("on Monday"), 1, finding.detail)
        self.assertNotIn("weekDays=", finding.detail)

    def test_the_first_spelling_wins_and_a_different_second_is_still_shown(self):
        # Taking the first must not hide a second that says something else.
        weekly = {"enabled": True, "configuration": {
            "type": "Weekly", "weekdays": ["Monday"], "weekDays": ["Tuesday"]}}
        (finding,) = self._schedule_findings(self._run(schedules=[weekly]))
        self.assertIn("on Monday", finding.detail)
        self.assertNotIn("on Tuesday", finding.detail)
        self.assertIn("weekDays=['Tuesday']", finding.detail)

    def test_the_camel_case_spelling_alone_is_rendered(self):
        weekly = {"enabled": True, "configuration": {
            "type": "Weekly", "weekDays": ["Friday"]}}
        (finding,) = self._schedule_findings(self._run(schedules=[weekly]))
        self.assertIn("on Friday", finding.detail)

    def test_a_disabled_schedule_is_mentioned_not_flagged(self):
        # Fabric does not run it either, so the unscheduled job behaves the
        # same; it is still named for whoever turns it back on.
        result = self._run(schedules=[dict(CRON, enabled=False)])
        (finding,) = self._schedule_findings(result)
        self.assertEqual(finding.severity, "info")
        self.assertIn("disabled in Fabric", finding.detail)
        self.assertFalse(result.needs_manual_review)

    def test_each_schedule_gets_its_own_finding(self):
        result = self._run(schedules=[CRON, dict(CRON, enabled=False)])
        self.assertEqual([f.severity for f in self._schedule_findings(result)],
                         ["flag", "info"])

    def test_an_unreadable_schedules_file_is_flagged_as_unknown(self):
        (finding,) = self._schedule_findings(self._run(
            schedules=[], schedules_error="cannot read .schedules: Expecting value"))
        self.assertEqual(finding.severity, "flag")
        self.assertIn("cannot read .schedules", finding.detail)
        self.assertIn("unknown", finding.detail)

    def test_no_schedule_no_finding(self):
        self.assertEqual(self._schedule_findings(self._run(schedules=[])), [])
        self.assertEqual(self._schedule_findings(self._run()), [])

    def test_a_blocked_pipeline_still_names_its_schedule(self):
        result = p2j.translate({"name": "Daily", "schedules": [CRON], "activities": [
            {"name": "Copy in", "type": "Copy", "notebook": "", "depends_on": []}]})
        self.assertEqual(result.translated_sql, "")
        self.assertEqual(len(self._schedule_findings(result)), 1)


class TranslationTests(unittest.TestCase):
    def test_a_notebook_activity_becomes_a_notebook_task(self):
        task = _payload(_run([_nb("Ingest", "01_Ingest")]))["tasks"][0]
        self.assertEqual(task["type"], "NOTEBOOK_TASK")
        self.assertEqual(task["notebookPath"], "/Workspace/01_Ingest.py")
        self.assertEqual(task["source"], "WORKSPACE")

    def test_depends_on_is_translated_to_task_keys(self):
        payload = _payload(_run([_nb("Ingest", "01_Ingest"),
                                 _nb("Aggregate", "02_Agg", ["Ingest"])]))
        self.assertEqual(payload["tasks"][1]["dependsOn"], [{"taskKey": "Ingest"}])

    def test_a_task_without_upstream_has_no_dependson_key(self):
        self.assertNotIn("dependsOn", _payload(_run([_nb("Ingest", "01_Ingest")]))["tasks"][0])

    def test_a_dependency_on_an_unknown_activity_is_dropped(self):
        # Referencing a task that is not in the job would be rejected by AIDP.
        payload = _payload(_run([_nb("Ingest", "01_Ingest", ["Ghost"])]))
        self.assertNotIn("dependsOn", payload["tasks"][0])

    def test_cluster_key_is_attached_when_given(self):
        task = _payload(_run([_nb("Ingest", "01_Ingest")], cluster_key="cl-1"))["tasks"][0]
        self.assertEqual(task["cluster"], {"clusterKey": "cl-1"})

    def test_a_missing_cluster_key_is_flagged_not_invented(self):
        result = _run([_nb("Ingest", "01_Ingest")])
        self.assertIn("PL12_NO_CLUSTER", [f.rule for f in result.findings])
        self.assertNotIn("cluster", _payload(result)["tasks"][0])

    def test_the_payload_is_valid_json_with_the_job_shape(self):
        payload = _payload(_run([_nb("Ingest", "01_Ingest")]))
        self.assertEqual(payload["name"], "Daily")
        self.assertEqual(payload["maxConcurrentRuns"], 1)
        self.assertIsInstance(payload["tasks"], list)


class RefusalTests(unittest.TestCase):
    """Both refusals are taken from the Databricks validator's clone_workflow."""

    def test_a_non_notebook_activity_blocks_the_whole_pipeline(self):
        # A job holding only the notebook tasks would run, skip the Copy that
        # fed them, and be wrong. Half of 30 real pipelines are in this bucket.
        result = _run([_nb("Ingest", "01_Ingest"),
                       {"name": "Copy in", "type": "Copy", "notebook": "",
                        "depends_on": []}])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("PL90_UNSUPPORTED_ACTIVITY", [f.rule for f in result.findings])
        self.assertIn("Copy", str(result.findings))

    def test_a_notebook_id_from_another_workspace_says_so(self):
        result = _run([_nb("Ingest", "0c96d0e9-ea78-4c25-848a-8c581c9c7b24")])
        self.assertEqual(result.translated_sql, "")
        detail = " ".join(f.detail for f in result.findings)
        self.assertIn("another workspace", detail)

    def test_an_unmigrated_notebook_blocks_rather_than_guessing_a_path(self):
        result = _run([_nb("Ingest", "99_Missing")])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("99_Missing", str(result.findings))

    def test_an_empty_pipeline_is_reported_not_emitted(self):
        result = _run([])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("PL92_EMPTY_PIPELINE", [f.rule for f in result.findings])

    def test_a_blocked_pipeline_still_reports_what_it_managed(self):
        result = _run([_nb("Ingest", "01_Ingest"),
                       {"name": "Copy in", "type": "Copy", "notebook": "",
                        "depends_on": []}])
        self.assertIn("PL10_NOTEBOOK_TASK", [f.rule for f in result.findings])


class FidelityTests(unittest.TestCase):
    """Fabric activity settings that used to be dropped on the way to AIDP."""

    def _after(self, conditions, **extra):
        downstream = dict(_nb("Next", "02_Agg", ["Ingest"]),
                          depends_on_conditions={"Ingest": conditions}, **extra)
        return _run([_nb("Ingest", "01_Ingest"), downstream])

    def test_a_failure_handler_runs_on_failure(self):
        # It became ALL_SUCCESS: the alert fired on success, never on failure.
        task = _payload(self._after(["Failed"]))["tasks"][1]
        self.assertEqual(task["runIf"], "ALL_FAILED")

    def test_completed_runs_either_way(self):
        self.assertEqual(_payload(self._after(["Completed"]))["tasks"][1]["runIf"], "ALL_DONE")
        self.assertEqual(_payload(self._after(["Succeeded", "Failed"]))["tasks"][1]["runIf"],
                         "ALL_DONE")

    def test_skipped_has_no_equivalent_and_blocks(self):
        result = self._after(["Skipped"])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("PL90_UNSUPPORTED_ACTIVITY", [f.rule for f in result.findings])

    def test_mixed_conditions_across_upstreams_block(self):
        c = dict(_nb("C", "02_Agg", ["A", "B"]),
                 depends_on_conditions={"A": ["Succeeded"], "B": ["Failed"]})
        result = _run([_nb("A", "01_Ingest"), _nb("B", "01_Ingest"), c])
        self.assertEqual(result.translated_sql, "")

    def test_the_activity_timeout_and_retries_are_kept(self):
        activity = dict(_nb("Ingest", "01_Ingest"), policy={
            "timeout": "0.02:30:00", "retry": 2, "retryIntervalInSeconds": 60})
        task = _payload(_run([activity]))["tasks"][0]
        self.assertEqual(task["timeoutSeconds"], 9000)
        self.assertEqual((task["maxRetries"], task["minRetryIntervalMillis"]), (2, 60000))

    def test_no_policy_means_fabrics_12h_default(self):
        self.assertEqual(_payload(_run([_nb("Ingest", "01_Ingest")]))["tasks"][0]
                         ["timeoutSeconds"], 43200)

    def test_an_unparsable_timeout_is_flagged(self):
        activity = dict(_nb("Ingest", "01_Ingest"), policy={"timeout": "@pipeline().x"})
        self.assertIn("PL13_TIMEOUT", [f.rule for f in _run([activity]).findings])

    def test_no_job_level_timeout_caps_the_chain(self):
        self.assertNotIn("timeoutSeconds", _payload(_run([_nb("Ingest", "01_Ingest")])))

    def test_equivalent_completed_forms_combine(self):
        c = dict(_nb("C", "02_Agg", ["A", "B"]), policy={},
                 depends_on_conditions={"A": ["Completed"], "B": ["Succeeded", "Failed"]})
        result = _run([_nb("A", "01_Ingest"), _nb("B", "01_Ingest"), c])
        self.assertEqual(_payload(result)["tasks"][2]["runIf"], "ALL_DONE")
        one = self._after(["Completed", "Succeeded"], policy={})
        self.assertEqual(_payload(one)["tasks"][1]["runIf"], "ALL_DONE")

    def test_an_inventory_without_conditions_is_flagged_stale(self):
        # A plan built before conditions were captured reads every edge as
        # Succeeded; say so instead of emitting it quietly.
        result = _run([_nb("Ingest", "01_Ingest"), _nb("Next", "02_Agg", ["Ingest"])])
        self.assertIn("PL15_STALE_INVENTORY", [f.rule for f in result.findings])
        fresh = self._after(["Succeeded"], policy={})
        self.assertNotIn("PL15_STALE_INVENTORY", [f.rule for f in fresh.findings])

    def test_an_expression_retry_is_flagged(self):
        activity = dict(_nb("Ingest", "01_Ingest"),
                        policy={"retry": {"value": "@int('2')", "type": "Expression"}})
        result = _run([activity])
        self.assertIn("PL13_RETRY", [f.rule for f in result.findings])
        self.assertEqual(_payload(result)["tasks"][0]["maxRetries"], 0)

    def test_run_if_follows_only_the_edges_the_job_keeps(self):
        alert = dict(_nb("Alert", "02_Agg", ["Ghost"]), policy={},
                     depends_on_conditions={"Ghost": ["Failed"]})
        result = _run([_nb("A", "01_Ingest"), alert])
        task = _payload(result)["tasks"][1]
        self.assertEqual((task["runIf"], "dependsOn" in task), ("ALL_SUCCESS", False))
        self.assertIn("PL16_DANGLING_EDGE", [f.rule for f in result.findings])

    def test_a_missing_same_workspace_notebook_is_not_called_cross_workspace(self):
        guid = "0c96d0e9-ea78-4c25-848a-8c581c9c7b24"
        same = dict(_nb("Ingest", guid),
                    notebook_workspace_id="00000000-0000-0000-0000-000000000000")
        detail = " ".join(f.detail for f in _run([same]).findings)
        self.assertIn("this workspace but not in the exported folder", detail)

    def test_an_upstream_on_success_and_on_failure_is_one_all_done_edge(self):
        # PL_DupEdge as Fabric writes it: TWO dependsOn entries for one
        # upstream, the canvas's on-success and on-failure arrows. Through
        # the inventory, because that is where the two became one: before
        # #18 it listed Ingest twice and kept the last entry's conditions,
        # and the job had two Ingest edges and ALL_FAILED. Now one edge,
        # and Succeeded + Failed is Completed.
        import tempfile
        from pathlib import Path
        from fabric_aidp.inventory import pipeline as inventory
        from fabric_aidp.inventory.git_workspace import discover_items
        body = {"properties": {"activities": [
            {"name": "Ingest", "type": "TridentNotebook", "dependsOn": [],
             "typeProperties": {"notebookName": "01_Ingest"}},
            {"name": "Next", "type": "TridentNotebook", "dependsOn": [
                {"activity": "Ingest", "dependencyConditions": ["Succeeded"]},
                {"activity": "Ingest", "dependencyConditions": ["Failed"]}],
             "typeProperties": {"notebookName": "02_Agg"}}]}}
        with tempfile.TemporaryDirectory() as tmp:
            item = Path(tmp) / "PL_DupEdge.DataPipeline"
            item.mkdir()
            (item / ".platform").write_text(json.dumps({
                "version": "2.0", "config": {"logicalId": "5b0c6a8e-1d2f-4c3b-9a7e-2f1e0d9c8b7a"},
                "metadata": {"type": "DataPipeline", "displayName": "PL_DupEdge"}}))
            (item / "pipeline-content.json").write_text(json.dumps(body))
            record = inventory.scan(discover_items(Path(tmp)))["items"]["pipelines"][0]
        result = p2j.translate(record, notebook_paths=PATHS)
        task = _payload(result)["tasks"][1]
        self.assertEqual(task["dependsOn"], [{"taskKey": "Ingest"}])
        self.assertEqual(task["runIf"], "ALL_DONE")
        self.assertIn("'Next' runs after Ingest", " ".join(f.detail for f in result.findings))

    def test_a_merged_upstream_does_not_hide_a_mixed_one(self):
        # runIf is one condition over every upstream: A finished (merged) and
        # B succeeded still have no single equivalent, as before.
        c = dict(_nb("C", "02_Agg", ["A", "B"]), policy={},
                 depends_on_conditions={"A": ["Failed", "Succeeded"], "B": ["Succeeded"]})
        result = _run([_nb("A", "01_Ingest"), _nb("B", "01_Ingest"), c])
        self.assertEqual(result.translated_sql, "")

    def test_an_old_inventory_repeating_an_upstream_blocks(self):
        # Before the merge the inventory listed A twice and kept the last
        # entry's conditions; emitting it ran B only when A failed.
        b = dict(_nb("B", "02_Agg", ["A", "A"]), policy={},
                 depends_on_conditions={"A": ["Failed"]})
        result = _run([_nb("A", "01_Ingest"), b])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("re-run `inventory`", " ".join(f.detail for f in result.findings))

    def test_a_repeated_upstream_with_no_conditions_at_all_is_also_stale(self):
        # Older still: no depends_on_conditions key. It used to raise PL90
        # alone, saying "this inventory kept only the last edge's
        # conditions" -- it kept none -- and PL15 never ran, because the
        # block returned before the staleness check.
        b = dict(_nb("B", "02_Agg", ["A", "A"]), policy={})
        result = _run([_nb("A", "01_Ingest"), b])
        self.assertEqual(result.translated_sql, "")
        rules = [f.rule for f in result.findings]
        self.assertIn("PL15_STALE_INVENTORY", rules)
        [blocked] = [f.detail for f in result.findings
                     if f.rule == "PL90_UNSUPPORTED_ACTIVITY"]
        self.assertNotIn("kept only the last edge's conditions", blocked)
        self.assertIn("records no dependency conditions", blocked)
        self.assertIn("re-run `inventory`", blocked)

    def test_a_repeated_upstream_with_conditions_says_the_last_entry_won(self):
        b = dict(_nb("B", "02_Agg", ["A", "A"]), policy={},
                 depends_on_conditions={"A": ["Failed"]})
        result = _run([_nb("A", "01_Ingest"), b])
        [blocked] = [f.detail for f in result.findings
                     if f.rule == "PL90_UNSUPPORTED_ACTIVITY"]
        self.assertIn("kept only the last edge's conditions", blocked)
        self.assertNotIn("PL15_STALE_INVENTORY", [f.rule for f in result.findings])

    def test_a_stale_activity_before_a_blocking_one_still_says_stale(self):
        # PL15 is about the inventory, not the activity that blocked: an
        # earlier activity read as stale has to be reported too.
        old = _nb("Next", "02_Agg", ["Ingest"])        # no conditions, no policy
        bad = dict(_nb("Copy", "01_Ingest"), type="Copy")
        result = _run([_nb("Ingest", "01_Ingest"), old, bad])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("PL15_STALE_INVENTORY", [f.rule for f in result.findings])

    def test_a_deactivated_activity_blocks(self):
        activity = dict(_nb("Ingest", "01_Ingest"), state="Inactive", on_inactive="Succeeded")
        result = _run([activity])
        self.assertEqual(result.translated_sql, "")
        self.assertIn("deactivated", str(result.findings))



class UnreadableDefinitionTests(unittest.TestCase):
    """"Empty" and "could not be read" arrived here as the same empty list.

    The inventory recorded a `content_error` for a pipeline whose
    pipeline-content.json would not open or would not parse, and the
    planner dropped it while carrying `schedules_error` -- one error
    field through, the other away. So the report said
    `PL92_EMPTY_PIPELINE  pipeline 'Broken' has no activities`. It is not
    empty; how many activities it has is unknown, and the two need
    different actions from the operator.
    """

    ERROR = ("cannot read pipeline-content.json: Expecting value: "
             "line 1 column 32 (char 31)")

    def _findings(self, **kw):
        return p2j.translate({"name": "Broken", "activities": [], **kw},
                             notebook_paths=PATHS).findings

    def test_an_unreadable_definition_is_not_reported_as_empty(self):
        rules = [f.rule for f in self._findings(content_error=self.ERROR)]
        self.assertNotIn("PL92_EMPTY_PIPELINE", rules)
        self.assertIn("PL93_DEFINITION_UNREADABLE", rules)

    def test_the_finding_carries_the_reason_the_inventory_recorded(self):
        finding = next(f for f in self._findings(content_error=self.ERROR)
                       if f.rule == "PL93_DEFINITION_UNREADABLE")
        self.assertEqual(finding.severity, "flag")
        self.assertIn("pipeline-content.json", finding.detail)
        self.assertIn("Expecting value", finding.detail)

    def test_the_finding_says_it_is_not_an_empty_pipeline(self):
        finding = next(f for f in self._findings(content_error=self.ERROR)
                       if f.rule == "PL93_DEFINITION_UNREADABLE")
        self.assertIn("not an empty pipeline", finding.detail)

    def test_a_genuinely_empty_pipeline_still_reports_PL92(self):
        rules = [f.rule for f in self._findings(content_error="")]
        self.assertIn("PL92_EMPTY_PIPELINE", rules)
        self.assertNotIn("PL93_DEFINITION_UNREADABLE", rules)

    def test_a_pipeline_with_no_content_error_field_at_all_reports_PL92(self):
        self.assertIn("PL92_EMPTY_PIPELINE", [f.rule for f in self._findings()])

    def test_a_schedule_error_and_a_content_error_are_both_reported(self):
        """They are different facts about the same item, and the planner
        used to carry exactly one of them."""
        rules = [f.rule for f in self._findings(
            content_error=self.ERROR,
            schedules_error="cannot read .schedules: Expecting value")]
        self.assertIn("PL20_SCHEDULE", rules)
        self.assertIn("PL93_DEFINITION_UNREADABLE", rules)



class TaskKeyUniquenessTests(unittest.TestCase):
    """A de-duplication suffix must not collide with a real activity name.

    `_unique_keys` appended `_2` to the second activity sharing a name. An
    activity genuinely called `A_2` already owns that key. Measured before
    this test existed, names ['A', 'A', 'A_2'] produced keys
    ['A', 'A_2', 'A_2'] -- two tasks with one taskKey, submitted to AIDP as
    a job. That is the exact failure the function's own docstring says it
    prevents.
    """

    def _keys(self, *names):
        keys, _ = p2j._unique_keys(
            [{"name": name} for name in names])
        return keys

    def test_a_suffix_does_not_take_a_real_activitys_key(self):
        self.assertEqual(self._keys("A", "A", "A_2"), ["A", "A_3", "A_2"])

    def test_it_keeps_skipping_until_the_key_is_free(self):
        self.assertEqual(self._keys("A", "A", "A_2", "A_3"),
                         ["A", "A_4", "A_2", "A_3"])

    def test_a_suffixed_name_can_itself_be_duplicated(self):
        self.assertEqual(self._keys("x", "x", "x_2", "x_2"),
                         ["x", "x_3", "x_2", "x_2_2"])

    def test_the_ordinary_duplicate_case_is_unchanged(self):
        self.assertEqual(self._keys("A", "A", "A"), ["A", "A_2", "A_3"])

    def test_two_spellings_of_one_key_still_separate(self):
        """`Load Meters` and `Load_Meters` clean to the same key, and did
        before this change too."""
        self.assertEqual(self._keys("Load Meters", "Load_Meters"),
                         ["Load_Meters", "Load_Meters_2"])

    def test_no_arrangement_produces_a_duplicate_key(self):
        for names in (["A", "A", "A_2"], ["A", "A_2", "A"], ["A_2", "A", "A"],
                      ["x", "x", "x_2", "x_2"], ["a b", "a_b", "a-b"],
                      ["", "", "task"], ["A", "A", "A", "A_2", "A_3", "A_4"]):
            with self.subTest(names=names):
                keys = self._keys(*names)
                self.assertEqual(len(keys), len(set(keys)), keys)


def _expr(text):
    return {"value": text, "type": "Expression"}


class ParameterTests(unittest.TestCase):
    """PL_Params, from the live probe workspace: every notebook parameter
    used to be dropped with no finding, and the notebook ran on its
    parameter-cell defaults."""

    PL_PARAMS = {"RunDate": {"type": "string", "defaultValue": "2026-09-01"}}

    def _one(self, parameters, pipeline_parameters=None):
        activity = dict(_nb("Load Day", "01_Ingest"), parameters=parameters)
        result = p2j.translate({"name": "PL_Params", "activities": [activity],
                                "parameters": pipeline_parameters or self.PL_PARAMS},
                               notebook_paths=PATHS, cluster_key="cl-1")
        return result, _payload(result)["tasks"][0]

    def _rules(self, result):
        return [f.rule for f in result.findings]

    def test_a_literal_is_carried_as_a_string(self):
        result, task = self._one({"limit": {"value": 100, "type": "int"},
                                  "region": {"value": "eu", "type": "string"},
                                  "full": {"value": True, "type": "bool"},
                                  "ratio": {"value": 0.5, "type": "float"}})
        self.assertEqual(task["parameters"], [
            {"name": "limit", "value": "100"}, {"name": "region", "value": "eu"},
            {"name": "full", "value": "True"}, {"name": "ratio", "value": "0.5"}])
        self.assertEqual(self._rules(result).count("PL21_NOTEBOOK_PARAMETER"), 4)

    def test_only_the_string_one_is_carried_without_losing_its_type(self):
        """Review note 8. The retype used to ride along inside PL21 as a
        parenthesis, at `rewrite`, which kept it out of REVIEW: the notebook
        got a `str` where Fabric gave an `int`, and the asset graded PASS.
        It is its own finding now, and `flag`, because the type comes back
        only if the notebook's parameters cell re-reads the name -- a
        different file, which this translator does not read."""
        result, _ = self._one({"limit": {"value": 100, "type": "int"},
                               "region": {"value": "eu", "type": "string"},
                               "full": {"value": True, "type": "bool"},
                               "ratio": {"value": 0.5, "type": "float"}})
        retyped = [f for f in result.findings if f.rule == "PL24_PARAMETER_RETYPED"]
        self.assertEqual(sorted(f.detail.split("parameter ")[1].split(" is")[0]
                                for f in retyped), ["'full'", "'limit'", "'ratio'"])
        self.assertTrue(all(f.severity == "flag" for f in retyped))
        self.assertIn("int in Fabric", str(result.findings))
        self.assertTrue(result.needs_manual_review)

    def test_a_string_parameter_stays_a_plain_rewrite(self):
        result, _ = self._one({"region": {"value": "eu", "type": "string"}})
        self.assertEqual(self._rules(result), ["PL21_NOTEBOOK_PARAMETER",
                                               "PL10_NOTEBOOK_TASK"])
        self.assertFalse(result.needs_manual_review)

    def test_an_undeclared_non_string_value_is_a_retype_too(self):
        """Fabric wrote `100` and said nothing about its type. The loss is
        the same and nothing records it, which is why the value is checked
        and not only the declaration."""
        result, _ = self._one({"limit": {"value": 100}})
        self.assertIn("PL24_PARAMETER_RETYPED", self._rules(result))
        self.assertIn("int in Fabric", str(result.findings))

    def test_a_bare_string_value_with_no_declaration_is_not_a_retype(self):
        result, _ = self._one({"region": "eu"})
        self.assertNotIn("PL24_PARAMETER_RETYPED", self._rules(result))

    def test_a_pipeline_parameter_is_fixed_at_its_default_and_flagged(self):
        for text in ("@pipeline().parameters.RunDate", "@{pipeline().parameters.RunDate}"):
            with self.subTest(expression=text):
                result, task = self._one({"run_date": {"value": _expr(text), "type": "string"}})
                self.assertEqual(task["parameters"], [{"name": "run_date", "value": "2026-09-01"}])
                self.assertIn("PL22_PARAMETER_DEFAULT", self._rules(result))
                self.assertTrue(result.needs_manual_review)

    def test_a_run_time_expression_is_flagged_not_invented(self):
        for text in ("@utcNow()", "@concat('a', 'b')", "@activity('Lk').output.value",
                     "@variables('v')", "@item().name", "@pipeline().RunId"):
            with self.subTest(expression=text):
                result, task = self._one({"stamp": {"value": _expr(text), "type": "string"}})
                self.assertNotIn("parameters", task)
                flagged = [f for f in result.findings if f.rule == "PL23_PARAMETER_EXPRESSION"]
                self.assertEqual(len(flagged), 1)
                self.assertIn("'stamp'", flagged[0].detail)
                self.assertIn(text, flagged[0].detail)
                self.assertTrue(result.needs_manual_review)

    def test_a_pipeline_parameter_without_a_default_is_flagged(self):
        result, task = self._one(
            {"a": {"value": _expr("@pipeline().parameters.NoDefault"), "type": "string"},
             "b": {"value": _expr("@pipeline().parameters.Undeclared"), "type": "string"}},
            {"NoDefault": {"type": "string"}})
        self.assertNotIn("parameters", task)
        self.assertEqual(self._rules(result).count("PL23_PARAMETER_EXPRESSION"), 2)
        self.assertIn("declares no parameter 'Undeclared'", str(result.findings))

    def test_the_probe_pipeline_keeps_what_it_can_and_flags_the_rest(self):
        # PL_Params verbatim: run_date <- RunDate, stamp <- @utcNow(), limit = 100.
        result, task = self._one({
            "run_date": {"value": _expr("@pipeline().parameters.RunDate"), "type": "string"},
            "stamp": {"value": _expr("@utcNow()"), "type": "string"},
            "limit": {"value": 100, "type": "int"}})
        self.assertEqual(task["parameters"], [{"name": "run_date", "value": "2026-09-01"},
                                              {"name": "limit", "value": "100"}])
        rules = self._rules(result)
        for rule in ("PL21_NOTEBOOK_PARAMETER", "PL22_PARAMETER_DEFAULT",
                     "PL23_PARAMETER_EXPRESSION"):
            self.assertIn(rule, rules)
        self.assertEqual(task["notebookPath"], "/Workspace/01_Ingest.py")

    def test_no_parameters_means_no_parameters_key(self):
        _, task = self._one({})
        self.assertNotIn("parameters", task)

    def test_an_inventory_without_parameters_is_flagged_stale(self):
        activity = _nb("Ingest", "01_Ingest")
        del activity["parameters"]
        self.assertIn("PL15_STALE_INVENTORY", [f.rule for f in _run([activity]).findings])
        self.assertNotIn("PL15_STALE_INVENTORY",
                         [f.rule for f in _run([_nb("Ingest", "01_Ingest")]).findings])

    def test_the_probe_export_through_inventory(self):
        import tempfile
        from pathlib import Path
        from fabric_aidp.inventory import pipeline as inventory
        from fabric_aidp.inventory.git_workspace import discover_items
        body = {"properties": {"activities": [{
            "name": "Load Day", "type": "TridentNotebook", "dependsOn": [],
            "typeProperties": {"notebookName": "NB_Load", "parameters": {
                "run_date": {"value": _expr("@pipeline().parameters.RunDate"),
                             "type": "string"},
                "stamp": {"value": _expr("@utcNow()"), "type": "string"},
                "limit": {"value": 100, "type": "int"}}},
            "policy": {"timeout": "0.02:00:00", "retry": 3,
                       "retryIntervalInSeconds": 120}}],
            "parameters": self.PL_PARAMS}}
        with tempfile.TemporaryDirectory() as tmp:
            item = Path(tmp) / "PL_Params.DataPipeline"
            item.mkdir()
            (item / ".platform").write_text(json.dumps({
                "version": "2.0", "config": {"logicalId": "e809cd6d-4aaf-4df0-b925-66b3f750acc1"},
                "metadata": {"type": "DataPipeline", "displayName": "PL_Params"}}))
            (item / "pipeline-content.json").write_text(json.dumps(body))
            record = inventory.scan(discover_items(Path(tmp)))["items"]["pipelines"][0]
        self.assertEqual(record["parameters"], self.PL_PARAMS)
        result = p2j.translate(record, notebook_paths={"NB_Load": "/Workspace/NB_Load.ipynb"},
                               cluster_key="cl-1")
        task = _payload(result)["tasks"][0]
        self.assertEqual([p["name"] for p in task["parameters"]], ["run_date", "limit"])
        self.assertNotIn("PL15_STALE_INVENTORY", self._rules(result))
        self.assertTrue(result.needs_manual_review)


class RootRunIfTests(unittest.TestCase):
    """AIDP JOB_VALIDATE_0028 (live): "Tasks ... with no dependencies should
    have RunIf condition as AllSuccess"."""

    def test_a_root_left_by_a_dropped_failure_edge_is_all_success(self):
        for conditions in (["Failed"], ["Completed"], ["Succeeded", "Failed"]):
            alert = dict(_nb("Alert", "02_Agg", ["Ghost"]), policy={},
                         depends_on_conditions={"Ghost": conditions})
            ambiguous = dict(_nb("After", "02_Agg", ["Run"]), policy={},
                             depends_on_conditions={"Run": conditions})
            with self.subTest(conditions=conditions):
                tasks = _payload(_run([_nb("Run", "01_Ingest"), _nb("Run", "01_Ingest"),
                                       alert, ambiguous]))["tasks"]
                for task in tasks:
                    if not task.get("dependsOn"):
                        self.assertEqual(task["runIf"], "ALL_SUCCESS", task["taskKey"])


if __name__ == "__main__":
    unittest.main()
