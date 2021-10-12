from flask_admin.contrib.sqla import ModelView


class RetryTaskAdminBase(ModelView):
    form_create_rules = ("task_type",)
    column_exclude_list = ("audit_data",)
    column_filters = (
        "status",
        "task_type.name",
        "task_type_key_values.task_type_key.name",
        "task_type_key_values.value",
    )
    column_searchable_list = ("retry_task_id", "task_type_key_values.value")
    column_display_pk = True
    column_list = (
        "task_type",
        "created_at",
        "updated_at",
        "attempts",
        "audit_data",
        "next_attempt_time",
        "status",
        "params",
    )
    column_details_list = (
        "task_type",
        "created_at",
        "updated_at",
        "attempts",
        "audit_data",
        "next_attempt_time",
        "status",
        "params",
    )


class TaskTypeAdminBase(ModelView):
    column_searchable_list = ("name",)
    form_columns = ("name", "path")


class TaskTypeKeyAdminBase(ModelView):
    column_searchable_list = ("name",)
    column_filters = ("task_type.name", "type")
    form_columns = ("task_type", "name", "type")


class TaskTypeKeyValueAdminBase(ModelView):
    column_searchable_list = ("value",)
    column_filters = ("task_type_key.task_type.name", "task_type_key.task_type_key_id", "retry_task.retry_task_id")
