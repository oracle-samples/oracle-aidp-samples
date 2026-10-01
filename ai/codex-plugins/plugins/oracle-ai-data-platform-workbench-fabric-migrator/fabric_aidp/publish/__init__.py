"""Optional: push migrated artifacts into an AIDP workspace.

Everything else this tool does is offline. This package is the one part that
talks to a live workspace, so it is opt-in, dry-run by default, and refuses to
overwrite anything it did not create.
"""
from fabric_aidp.publish.publisher import PublishError, plan_publish, publish

__all__ = ["PublishError", "plan_publish", "publish"]
