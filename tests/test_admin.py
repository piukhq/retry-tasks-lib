# pylint: disable=too-many-arguments

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from sqlalchemy import func
from sqlalchemy.future import select

from retry_tasks_lib.admin.views import RetryTaskAdminBase
from retry_tasks_lib.db.models import RetryTask, TaskType, TaskTypeKeyValue
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import sync_create_task

if TYPE_CHECKING:
    from redis import Redis
    from sqlalchemy.orm import Session


@pytest.fixture()
def admin(sync_db_session: "Session", redis: "Redis") -> RetryTaskAdminBase:
    adm = RetryTaskAdminBase(RetryTask, sync_db_session, name="whatever", endpoint="whatever")
    adm.redis = redis
    return adm


@mock.patch("retry_tasks_lib.admin.views.flash")
def test_retry_task_admin_requeue_action(
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
) -> None:

    expected_tasks_count = 0
    for status in RetryTaskStatuses.requeueable_statuses_names():
        lookup_val = f"{status}-task"
        retry_task = sync_create_task(
            sync_db_session,
            task_type_name=task_type_with_keys_sync.name,
            params={"lookup-val": lookup_val, "task-type-key-int": 42},
        )
        retry_task.status = status
        sync_db_session.commit()
        expected_tasks_count += 1

        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == expected_tasks_count

        admin.action_requeue_tasks([retry_task.retry_task_id])
        expected_tasks_count += 1
        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == expected_tasks_count
        new_task = (
            sync_db_session.execute(
                select(RetryTask).where(
                    RetryTask.retry_task_id != retry_task.retry_task_id,
                    RetryTask.retry_task_id == TaskTypeKeyValue.retry_task_id,
                    TaskTypeKeyValue.value == lookup_val,
                )
            )
            .unique()
            .scalar_one()
        )
        assert new_task.get_params() == {"lookup-val": lookup_val, "task-type-key-int": 42}
        assert new_task.status == RetryTaskStatuses.PENDING
        mock_flash.assert_called_with("Requeued selected ['FAILED', 'CANCELLED'] tasks")


@mock.patch("retry_tasks_lib.admin.views.flash")
def test_retry_task_admin_requeue_action_status_not_allowed(
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
        if status.name in RetryTaskStatuses.requeueable_statuses_names():
            continue

        retry_task.status = status
        sync_db_session.commit()

        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 1

        admin.action_requeue_tasks([retry_task.retry_task_id])
        mock_flash.assert_called_with("No relevant ['FAILED', 'CANCELLED'] tasks to requeue.", category="error")

        assert sync_db_session.execute(select(func.count()).select_from(RetryTask)).scalar_one() == 1


@mock.patch("retry_tasks_lib.admin.views.enqueue_many_retry_tasks")
@mock.patch("retry_tasks_lib.admin.views.flash")
def test_retry_task_admin_requeue_action_at_front(
    mock_flash: mock.MagicMock,
    mock_enqueue_many_retry_tasks: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
    redis: "Redis",
) -> None:

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
    assert new_task.status == RetryTaskStatuses.PENDING
    mock_enqueue_many_retry_tasks.assert_called_once_with(
        sync_db_session, connection=redis, retry_tasks_ids=[new_task.retry_task_id], at_front=True
    )
    mock_flash.assert_called()


@mock.patch("retry_tasks_lib.admin.views.flash")
def test_retry_task_admin_cancel_action(
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
) -> None:

    for status in RetryTaskStatuses.cancellable_statuses_names():
        retry_task = sync_create_task(
            sync_db_session,
            task_type_name=task_type_with_keys_sync.name,
            params={"task-type-key-int": 42},
        )
        retry_task.status = status
        sync_db_session.commit()

        admin.action_cancel_tasks([retry_task.retry_task_id])

        sync_db_session.refresh(retry_task)

        assert retry_task.status == RetryTaskStatuses.CANCELLED
        mock_flash.assert_called_with("Cancelled selected elegible tasks")


@mock.patch("retry_tasks_lib.admin.views.flash")
def test_retry_task_admin_cancel_action_status_not_allowed(
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_sync: TaskType,
) -> None:

    cancellable_statuses = RetryTaskStatuses.cancellable_statuses_names()
    retry_task = sync_create_task(
        sync_db_session,
        task_type_name=task_type_with_keys_sync.name,
        params={"task-type-key-int": 42},
    )

    for status in RetryTaskStatuses:
        if status.name in cancellable_statuses:
            continue

        retry_task.status = status
        sync_db_session.commit()

        admin.action_cancel_tasks([retry_task.retry_task_id])
        mock_flash.assert_called_with(
            f"No elegible task to cancel. Tasks must be in one of the following states: {cancellable_statuses}",
            category="error",
        )


@mock.patch("retry_tasks_lib.admin.views.flash")
@mock.patch("retry_tasks_lib.admin.views.enqueue_many_retry_tasks")
def test_retry_task_admin_cancel_action_with_cleanup(
    mock_enqueue_many_retry_tasks: mock.MagicMock,
    mock_flash: mock.MagicMock,
    sync_db_session: "Session",
    admin: RetryTaskAdminBase,
    task_type_with_keys_and_cleanup_handler_path: TaskType,
    redis: "Redis",
) -> None:

    for status in RetryTaskStatuses.cancellable_statuses_names():
        retry_task = sync_create_task(
            sync_db_session,
            task_type_name=task_type_with_keys_and_cleanup_handler_path.name,
            params={"task-type-key-int": 42},
        )
        retry_task.status = status
        sync_db_session.commit()

        admin.action_cancel_tasks([retry_task.retry_task_id])

        sync_db_session.refresh(retry_task)

        assert retry_task.status == RetryTaskStatuses.CLEANUP
        mock_enqueue_many_retry_tasks.assert_called_with(
            db_session=sync_db_session,
            connection=redis,
            retry_tasks_ids=[retry_task.retry_task_id],
            use_task_type_exc_handler=False,
        )
        mock_flash.assert_called_with("Started clean up jobs for selected tasks")
