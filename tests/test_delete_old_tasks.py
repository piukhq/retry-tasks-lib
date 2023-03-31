from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.scheduled.cleanup import delete_old_task_data

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from sqlalchemy.orm import Session


def test_delete_old_task_data(
    create_mock_task: "Callable[..., RetryTask]", sync_db_session: "Session", mocker: "MockerFixture"
) -> None:

    mock_logger = mocker.patch("retry_tasks_lib.scheduled.cleanup.logger")

    now = datetime.now(tz=UTC)

    deleteable_task = create_mock_task({"status": RetryTaskStatuses.SUCCESS})
    deleteable_task.created_at = now - timedelta(days=181)
    deleteable_task_id = deleteable_task.retry_task_id

    wrong_status_task = create_mock_task({"status": RetryTaskStatuses.FAILED})
    wrong_status_task.created_at = now - timedelta(days=200)

    not_old_enough_task = create_mock_task({"status": RetryTaskStatuses.SUCCESS})
    not_old_enough_task.created_at = now - timedelta(days=10)

    sync_db_session.commit()

    tz = ZoneInfo("Europe/London")

    mock_time_reference = datetime.now(tz=tz).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=180)

    delete_old_task_data(db_session=sync_db_session, time_reference=mock_time_reference)

    sync_db_session.expire_all()

    logger_calls = mock_logger.info.call_args_list

    assert logger_calls[0].args == ("Cleaning up tasks created before %s...", mock_time_reference.date())
    assert logger_calls[1].args == ("Deleted %d tasks. ( °╭ ︿ ╮°)", 1)

    assert not sync_db_session.get(RetryTask, deleteable_task_id)
    assert wrong_status_task.retry_task_id
    assert not_old_enough_task.retry_task_id
