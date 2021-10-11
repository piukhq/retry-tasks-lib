from flask import Markup, url_for

from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue  # noqa: F401


@property  # type: ignore
def formatted_params(self) -> str:  # type: ignore
    result = "<p>"
    for value in self.task_type_key_values:
        key = value.task_type_key
        result += '<strong><a href="{0}">{1}</a></strong>: {2}</br>'.format(
            url_for("task_type_key_values.details_view", id=f"{value.retry_task_id},{value.task_type_key_id}"),
            key.name,
            value.value,
        )

    result += "</p>"
    return Markup(result)


setattr(RetryTask, "params", formatted_params)
