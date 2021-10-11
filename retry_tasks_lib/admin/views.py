class RetryTaskAdmin:
    column_exclude_list = ("response_data",)
    column_filters = (
        "retry_status",
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
        "response_data",
        "next_attempt_time",
        "retry_status",
        "task_params",
    )

    column_details_list = (
        "task_type",
        "created_at",
        "updated_at",
        "attempts",
        "response_data",
        "next_attempt_time",
        "retry_status",
        "task_params",
    )


class TaskTypeAdmin:
    pass


class TaskTypeKeyAdmin:
    pass


class TaskTypeKeyValueAdmin:
    pass


def get_views(base: type) -> dict:
    return {
        view.__name__: type(view.__name__, (base,), view.__dict__)
        for view in (RetryTaskAdmin, TaskTypeAdmin, TaskTypeKeyAdmin, TaskTypeKeyValueAdmin)
    }
