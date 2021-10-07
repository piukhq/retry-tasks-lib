from unittest.mock import AsyncMock, MagicMock

import pytest

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses


@pytest.fixture(scope="function")
def retry_task() -> RetryTask:
    return RetryTask(
        retry_task_id=1,
        task_type_id=1,
        retry_status=RetryTaskStatuses.PENDING,
        attempts=0,
        response_data=[],
        next_attempt_time=None,
        task_type=MagicMock(name="TaskType"),
    )


@pytest.fixture(scope="function")
def async_db_session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


@pytest.fixture(scope="function")
def db_session() -> AsyncMock:
    return MagicMock(spec=Session)
