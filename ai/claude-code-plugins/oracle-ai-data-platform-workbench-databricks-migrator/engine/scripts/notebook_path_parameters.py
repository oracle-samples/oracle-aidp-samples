"""Shared precedence for workflow, run, CLI, and task parameter bindings."""


def update_parameters(target, incoming):
    """Apply a layer; an empty notebook_path is an absent binding."""
    for key, value in incoming.items():
        if key == "notebook_path" and value == "":
            continue
        target[key] = value
    return target


def effective_task_parameters(task_parameters, job_parameters):
    """Return the runtime binding, treating an empty notebook_path as absent."""
    effective = update_parameters(dict(task_parameters), job_parameters)
    if effective.get("notebook_path") == "":
        effective.pop("notebook_path")
    return effective
