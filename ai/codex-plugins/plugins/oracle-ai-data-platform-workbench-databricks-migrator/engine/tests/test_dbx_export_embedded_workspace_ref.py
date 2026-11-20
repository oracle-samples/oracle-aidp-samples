from dbx_export.sanitizer import rewrite_embedded_workspace_ref


def test_rewrites_second_users_root_from_already_rooted_path():
    malformed = (
        "/Users/user9@example.com/"
        "Users/user2@example.com/ReadTributeRequestLogsBAUandBatch"
    )
    assert rewrite_embedded_workspace_ref(malformed) == (
        "/Users/user2@example.com/ReadTributeRequestLogsBAUandBatch"
    )


def test_rewrites_repeated_roots_after_export_prefix():
    malformed = (
        "/Workspace/scratch/TEST_USER/AI_MIGRATION/migration_folder/"
        "dbx_export/notebooks/Users/user9@example.com/"
        "Users/user2@example.com/ReadTributeRequestLogsBAUandBatch.ipynb"
    )
    assert rewrite_embedded_workspace_ref(malformed) == (
        "/Users/user2@example.com/"
        "ReadTributeRequestLogsBAUandBatch.ipynb"
    )


def test_rewrites_embedded_root_from_non_workspace_prefix():
    assert rewrite_embedded_workspace_ref(
        "/Repos/team/project/helpers/Shared/common/Utility"
    ) == "/Shared/common/Utility"


def test_keeps_valid_single_workspace_root_unchanged():
    valid = "/Users/user2@example.com/ReadTributeRequestLogsBAUandBatch"
    assert rewrite_embedded_workspace_ref(valid) == valid
