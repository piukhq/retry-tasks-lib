from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
from sqlalchemy.orm import Session

from .models import RetryTask, TaskType, TaskTypeKey, TaskTypeKeyValue
from .views import get_views


def load_admin_views(admin: Admin, db_session: Session, auth_base: ModelView, category: str) -> None:
    views = get_views(auth_base)

    admin.add_view(
        views["RetryTaskAdmin"](RetryTask, db_session, "RetryTasks", endpoint="retry_tasks", category=category)
    )
    admin.add_view(views["TaskTypeAdmin"](TaskType, db_session, "TaskTypes", endpoint="task_types", category=category))
    admin.add_view(
        views["TaskTypeKeyAdmin"](TaskTypeKey, db_session, "TaskTypeKeys", endpoint="task_type_keys", category=category)
    )
    admin.add_view(
        views["TaskTypeKeyValueAdmin"](
            TaskTypeKeyValue, db_session, "TaskTypeKeyValues", endpoint="task_type_key_values", category=category
        )
    )
