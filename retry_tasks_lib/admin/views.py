from typing import Dict

views_values: Dict[str, dict] = {
    "RetryTaskAdmin": dict(
        column_exclude_list=("audit_data",),
        column_filters=(
            "status",
            "task_type.name",
            "task_type_key_values.task_type_key.name",
            "task_type_key_values.value",
        ),
        column_searchable_list=("retry_task_id", "task_type_key_values.value"),
        column_display_pk=True,
        column_list=(
            "task_type",
            "created_at",
            "updated_at",
            "attempts",
            "audit_data",
            "next_attempt_time",
            "status",
            "params",
        ),
        column_details_list=(
            "task_type",
            "created_at",
            "updated_at",
            "attempts",
            "audit_data",
            "next_attempt_time",
            "status",
            "params",
        ),
    ),
    "TaskTypeAdmin": dict(),
    "TaskTypeKeyAdmin": dict(),
    "TaskTypeKeyValueAdmin": dict(),
}


def get_views(base: type) -> dict:
    return {name: type(name, (base,), values) for name, values in views_values.items()}
