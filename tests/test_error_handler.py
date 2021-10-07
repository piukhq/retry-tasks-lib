from datetime import datetime, timedelta
from typing import Generator
from unittest import mock

import pytest
import requests
import rq

from pytest_mock import MockerFixture

from retry_task_lib.db.models import RetryTask
from retry_task_lib.enums import QueuedRetryStatuses
from retry_task_lib.utils.error_handler import handle_request_exception

now = datetime.utcnow()


@pytest.fixture(scope="function")
def handle_request_exception_params(db_session: mock.MagicMock, retry_task: RetryTask) -> dict:
    return {
        "db_session": db_session,
        "queue": "test_queue",
        "connection": mock.MagicMock(name="connection"),
        "action": mock.MagicMock(name="action"),
        "backoff_base": 3,
        "max_retries": 3,
        "job": mock.MagicMock(spec=rq.job.Job, kwargs={"retry_task_id": retry_task.retry_task_id}),
    }


@pytest.fixture(scope="function")
def errored_retry_task(retry_task: RetryTask) -> RetryTask:
    retry_task.attempts = 1
    retry_task.retry_status = QueuedRetryStatuses.IN_PROGRESS
    retry_task.next_attempt_time = now
    return retry_task


@pytest.fixture(scope="function")
def fixed_now() -> Generator[datetime, None, None]:
    with mock.patch("retry_task_lib.utils.error_handler.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = now
        yield now


def test_handle_adjust_balance_error_5xx(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_task_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_task_lib.utils.error_handler.enqueue_task", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_get_task = mocker.patch("retry_task_lib.utils.error_handler.get_retry_task")
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

    assert len(errored_retry_task.response_data) == 1
    assert errored_retry_task.response_data[0]["response"]["body"] == "Internal server error"
    assert errored_retry_task.response_data[0]["response"]["status"] == 500
    mock_flag_modified.assert_called_once()
    mock_enqueue.assert_called_once_with(
        queue=handle_request_exception_params["queue"],
        connection=handle_request_exception_params["connection"],
        action=handle_request_exception_params["action"],
        retry_task=errored_retry_task,
        backoff_seconds=180.0,
    )
    assert errored_retry_task.retry_status == QueuedRetryStatuses.IN_PROGRESS
    assert errored_retry_task.attempts == 1
    assert errored_retry_task.next_attempt_time == fixed_now + timedelta(seconds=180)


def test_handle_adjust_balance_error_no_response(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_task_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_task_lib.utils.error_handler.enqueue_task", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_get_task = mocker.patch("retry_task_lib.utils.error_handler.get_retry_task")
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

    assert len(errored_retry_task.response_data) == 1
    assert errored_retry_task.response_data[0]["error"] == "Request timed out"
    mock_flag_modified.assert_called_once()
    mock_enqueue.assert_called_once_with(
        queue=handle_request_exception_params["queue"],
        connection=handle_request_exception_params["connection"],
        action=handle_request_exception_params["action"],
        retry_task=errored_retry_task,
        backoff_seconds=180.0,
    )
    assert errored_retry_task.retry_status == QueuedRetryStatuses.IN_PROGRESS
    assert errored_retry_task.attempts == 1
    assert errored_retry_task.next_attempt_time == fixed_now + timedelta(seconds=180)


def test_handle_adjust_balance_error_no_further_retries(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    errored_retry_task.attempts = handle_request_exception_params["max_retries"]
    mock_flag_modified = mocker.patch("retry_task_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_task_lib.utils.error_handler.enqueue_task", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_task_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_task_lib.utils.error_handler.get_retry_task")
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
    assert errored_retry_task.retry_status == QueuedRetryStatuses.FAILED
    assert errored_retry_task.attempts == handle_request_exception_params["max_retries"]
    assert errored_retry_task.next_attempt_time is None


def test_handle_adjust_balance_error_unhandleable_response(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_task_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_task_lib.utils.error_handler.enqueue_task", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_task_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_task_lib.utils.error_handler.get_retry_task")
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
    assert errored_retry_task.retry_status == QueuedRetryStatuses.FAILED
    assert errored_retry_task.next_attempt_time is None


def test_handle_adjust_balance_error_unhandled_exception(
    errored_retry_task: RetryTask, fixed_now: datetime, handle_request_exception_params: dict, mocker: MockerFixture
) -> None:
    mock_flag_modified = mocker.patch("retry_task_lib.db.models.flag_modified")
    mock_enqueue = mocker.patch(
        "retry_task_lib.utils.error_handler.enqueue_task", return_value=fixed_now + timedelta(seconds=180)
    )
    mock_sentry = mocker.patch("retry_task_lib.utils.error_handler.sentry_sdk")
    mock_get_task = mocker.patch("retry_task_lib.utils.error_handler.get_retry_task")
    mock_get_task.return_value = errored_retry_task
    handle_request_exception(
        **handle_request_exception_params,
        exc_value=ValueError("Resistance is futile."),
    )

    mock_enqueue.assert_not_called()
    mock_sentry.capture_exception.assert_called_once()
    mock_flag_modified.assert_not_called()
    assert errored_retry_task.retry_status == QueuedRetryStatuses.FAILED
    assert errored_retry_task.next_attempt_time is None
