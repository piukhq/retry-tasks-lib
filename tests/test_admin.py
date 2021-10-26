from typing import TYPE_CHECKING
from unittest import mock

import pytest
import rq

from sqlalchemy import func
from sqlalchemy.future import select

from retry_tasks_lib.admin.views import RetryTaskAdminBase
from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import sync_create_task

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture()
def admin(sync_db_session: "Session") -> RetryTaskAdminBase:
    admin = RetryTaskAdminBase(RetryTask, sync_db_session, name="whatever", endpoint="whatever")
    admin.redis = mock.Mock()
    return admin


@mock.patch("retry_tasks_lib.admin.views.flash")
@mock.patch("rq.Queue", spec=rq.Queue)
def test_retry_task_admin_requeue_action(
    mock_queue_cls: mock.MagicMock,
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
) -> None:
    mock_queue_obj = mock.Mock()
    mock_queue_obj.enqueue_many.return_value = [mock.Mock(name="fake rq.job.Job")]
    mock_queue_cls.return_value = mock_queue_obj

    retry_task = sync_create_task(
        sync_db_session,
        task_type_name=task_type_with_keys_sync.name,
        params={"task-type-key-str": "a-string", "task-type-key-int": 42},
    )
    retry_task.status = "FAILED"
    sync_db_session.commit()

    assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 1

    admin.action_requeue_tasks([retry_task.retry_task_id])

    assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 2
    new_task = (
        sync_db_session.execute(select(RetryTask).where(RetryTask.retry_task_id != retry_task.retry_task_id))
        .unique()
        .scalar_one()
    )
    assert new_task.get_params() == {"task-type-key-str": "a-string", "task-type-key-int": 42}
    assert new_task.status.name == "IN_PROGRESS"
    mock_queue_cls.assert_called_once_with(retry_task.task_type.queue_name, connection=admin.redis)
    mock_flash.assert_called_once_with("Requeued 1 FAILED tasks")


@mock.patch("retry_tasks_lib.admin.views.flash")
@mock.patch("rq.Queue", spec=rq.Queue)
def test_retry_task_admin_requeue_action_status_not_allowed(
    mock_queue_cls: mock.MagicMock,
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
) -> None:

    retry_task = sync_create_task(
        sync_db_session,
        task_type_name=task_type_with_keys_sync.name,
        params={"task-type-key-str": "a-string", "task-type-key-int": 42},
    )

    for status in RetryTaskStatuses:
        if status == RetryTaskStatuses.FAILED:
            continue
        retry_task.status = status
        sync_db_session.commit()

        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 1

        admin.action_requeue_tasks([retry_task.retry_task_id])
        mock_flash.assert_called_with("No relevant (FAILED) tasks to requeue.", category="error")

        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 1
