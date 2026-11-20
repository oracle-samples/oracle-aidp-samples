from types import SimpleNamespace
import yaml
import pytest
from dbx_export.customer_cluster_libraries import prepare_cluster_library_rules, filter_cluster_library_refs, load_cluster_library_rules
from test_customer_cluster_libraries import graph, Aidp, report, REPO, OLD, FIXED


def test_two_replacements_same_cluster_keep_other_cluster(tmp_path):
    rules=[{'dbx_job_id':'123','job_cluster_key':'scoring','jar':jar,
            'replace_basenames':[pattern],'reason':'operator-approved'}
           for jar,pattern in [('engine-assembly-fixed.jar','engine*.jar'),('feature-fixed.jar','feature*.jar')]]
    (tmp_path/'overrides.yaml').write_text(yaml.safe_dump({'schema_version':1,'overrides':[],'cluster_libraries':rules}))
    cfg=SimpleNamespace(customizations=SimpleNamespace(path=str(tmp_path)))
    aidp=Aidp()
    aidp.list_workspace_objects=lambda _: [{'path':FIXED,'type':'FILE'},{'path':REPO+'/feature-fixed.jar','type':'FILE'}]
    plan,contracts=prepare_cluster_library_rules(aidp,graph(),cfg,REPO,{},lambda _:None,report())
    assert len(contracts)==2
    refs=[{'jar':OLD},{'jar':'feature-old.jar'},{'jar':FIXED},{'jar':REPO+'/feature-fixed.jar'}]
    assert filter_cluster_library_refs(refs,plan,'job:123/job_cluster:scoring')==refs[2:]
    assert filter_cluster_library_refs(refs,plan,'job:123/job_cluster:other')==refs


@pytest.mark.parametrize("new", [False, True])
def test_both_contracts_reach_real_phase3(tmp_path, monkeypatch, new):
    import test_customer_cluster_libraries as existing
    instances = []
    class TwoJarAidp(existing.Aidp):
        def __init__(self):
            super().__init__()
            instances.append(self)
        def list_workspace_objects(self, path):
            return super().list_workspace_objects(path) + [{"path": REPO + "/feature-fixed.jar", "type": "FILE"}]
    original_pack = existing.pack
    def two_jar_pack(path):
        cfg = original_pack(path)
        file = path / "overrides.yaml"
        doc = yaml.safe_load(file.read_text())
        doc["cluster_libraries"].append({
            "dbx_job_id": "123", "job_cluster_key": "scoring", "jar": "feature-fixed.jar",
            "replace_basenames": ["feature*.jar"], "reason": "operator-approved second family",
        })
        file.write_text(yaml.safe_dump(doc))
        return cfg
    monkeypatch.setattr(existing, "Aidp", TwoJarAidp)
    monkeypatch.setattr(existing, "pack", two_jar_pack)
    existing.test_real_phase3_installs_override_only_on_actual_scoring_cluster(tmp_path, monkeypatch, new)
    paths = {item["path"] for cluster, items in instances[-1].calls if cluster == "TEST_scoring" for item in items}
    assert {FIXED, REPO + "/feature-fixed.jar"} <= paths
    assert not any(item["path"] == REPO + "/feature-fixed.jar" for cluster, items in instances[-1].calls if cluster == "TEST_other" for item in items)
