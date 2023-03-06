from datetime import UTC, datetime
from typing import TYPE_CHECKING

from retry_tasks_lib.utils.synchronous import sync_create_task

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from retry_tasks_lib.db.models import TaskType


def test_get_params(sync_db_session: "Session", task_type_with_keys_sync: "TaskType") -> None:

    now = datetime.now(tz=UTC)
    input_params = {
        "task-type-key-str": "I am a string",
        "task-type-key-int": 1234,
        "task-type-key-float": 12.34,
        "task-type-key-bool": False,
        "task-type-key-date": now.date(),
        "task-type-key-datetime": now,
    }

    retry_task = sync_create_task(sync_db_session, task_type_name=task_type_with_keys_sync.name, params=input_params)

    task_type_key_values = retry_task.task_type_key_values

    assert len(task_type_key_values) == len(input_params)
    for task_type_key_value in retry_task.task_type_key_values:
        assert isinstance(task_type_key_value.value, str)

    task_params = retry_task.get_params()

    for k, v in task_params.items():
        assert input_params[k] == v
