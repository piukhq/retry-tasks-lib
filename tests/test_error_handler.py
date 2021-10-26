from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest
import requests
import rq

from pytest_mock import MockerFixture

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.error_handler import handle_request_exception

now = datetime.utcnow()
if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture(scope="function")
def errored_retry_task(retry_task_sync: RetryTask, sync_db_session: "Session") -> RetryTask:
    retry_task_sync.attempts = 1
    retry_task_sync.status = RetryTaskStatuses.IN_PROGRESS
    retry_task_sync.next_attempt_time = now
    sync_db_session.commit()
    return retry_task_sync


@pytest.fixture(scope="function")
def handle_request_exception_params(mock_sync_db_session: mock.MagicMock, errored_retry_task: RetryTask) -> dict:
    return {
        "db_session": mock_sync_db_session,
        "connection": mock.MagicMock(name="connection"),
        "backoff_base": 3,
        "max_retries": 3,
        "job": mock.MagicMock(spec=rq.job.Job, kwargs={"retry_task_id": errored_retry_task.retry_task_id}),
    }


@pytest.fixture(scope="function")
def fixed_now() -> Generator[datetime, None, None]:
    with mock.patch("retry_tasks_lib.utils.error_handler.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = now
        yield now


def test_handle_error_http_4xx(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_tasks_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_tasks_lib.utils.error_handler.enqueue_retry_task_delay", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_tasks_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_tasks_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    mock_request = mock.MagicMock(spec=requests.Request, url="http://test.url")
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=requests.RequestException(
            request=mock_request,
            response=mock.MagicMock(spec=requests.Response, request=mock_request, status_code=401, text="Unauthorized"),
        ),
    )

    mock_enqueue.assert_not_called()
    mock_sentry.capture_message.assert_not_called()
    mock_flag_modified.assert_called_once()
    assert errored_retry_task.status == RetryTaskStatuses.FAILED
    assert errored_retry_task.next_attempt_time is None


def test_handle_error_http_5xx(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_tasks_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_tasks_lib.utils.error_handler.enqueue_retry_task_delay", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_get_task = mocker.patch("retry_tasks_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    mock_request = mock.MagicMock(spec=requests.Request, url="http://test.url")
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=requests.RequestException(
            request=mock_request,
            response=mock.MagicMock(
                spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
            ),
        ),
    )

    assert len(errored_retry_task.audit_data) == 1
    assert errored_retry_task.audit_data[0]["response"]["body"] == "Internal server error"
    assert errored_retry_task.audit_data[0]["response"]["status"] == 500
    mock_flag_modified.assert_called_once()
    mock_enqueue.assert_called_once_with(
        connection=handle_request_exception_params["connection"],
        retry_task=errored_retry_task,
        delay_seconds=180.0,
    )
    assert errored_retry_task.status == RetryTaskStatuses.IN_PROGRESS
    assert errored_retry_task.attempts == 1
    assert errored_retry_task.next_attempt_time == fixed_now + timedelta(seconds=180)


def test_handle_error_http_timeout(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_tasks_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_tasks_lib.utils.error_handler.enqueue_retry_task_delay", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_get_task = mocker.patch("retry_tasks_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    mock_request = mock.MagicMock(spec=requests.Request, url="http://test.url")
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=requests.Timeout(
            "Request timed out",
            request=mock_request,
            response=None,
        ),
    )

    assert len(errored_retry_task.audit_data) == 1
    assert errored_retry_task.audit_data[0]["error"] == "Request timed out"
    mock_flag_modified.assert_called_once()
    mock_enqueue.assert_called_once_with(
        connection=handle_request_exception_params["connection"],
        retry_task=errored_retry_task,
        delay_seconds=180.0,
    )
    assert errored_retry_task.status == RetryTaskStatuses.IN_PROGRESS
    assert errored_retry_task.attempts == 1
    assert errored_retry_task.next_attempt_time == fixed_now + timedelta(seconds=180)


def test_handle_http_error_no_further_retries(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    errored_retry_task.attempts = handle_request_exception_params["max_retries"]
    mock_flag_modified = mocker.patch("retry_tasks_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_tasks_lib.utils.error_handler.enqueue_retry_task_delay", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_tasks_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_tasks_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    mock_request = mock.MagicMock(spec=requests.Request, url="http://test.url")
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=requests.RequestException(
            request=mock_request,
            response=mock.MagicMock(
                spec=requests.Response, request=mock_request, status_code=500, text="Internal server error"
            ),
        ),
    )

    mock_enqueue.assert_not_called()
    mock_sentry.capture_message.assert_called_once()
    mock_flag_modified.assert_called_once()
    assert errored_retry_task.status == RetryTaskStatuses.FAILED
    assert errored_retry_task.attempts == handle_request_exception_params["max_retries"]
    assert errored_retry_task.next_attempt_time is None


def test_handle_error_unhandled_exception(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_tasks_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_tasks_lib.utils.error_handler.enqueue_retry_task_delay", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_tasks_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_tasks_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=ValueError("Resistance is futile."),
    )

    mock_enqueue.assert_not_called()
    mock_sentry.capture_exception.assert_called_once()
    mock_flag_modified.assert_not_called()
    assert errored_retry_task.status == RetryTaskStatuses.FAILED
    assert errored_retry_task.next_attempt_time is None
