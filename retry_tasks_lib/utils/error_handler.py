from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

import requests
import rq
import sentry_sdk

from sqlalchemy.orm.session import Session

from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses

from . import logger
from .synchronous import enqueue_retry_task_delay, get_retry_task


def _handle_request_exception(
    queue: str,
    connection: Any,
    action: Callable,
    backoff_base: int,
    max_retries: int,
    retry_task: RetryTask,
    request_exception: requests.RequestException,
) -> Tuple[dict, Optional[RetryTaskStatuses], Optional[datetime]]:
    status = None
    next_attempt_time = None
    subject = retry_task.task_type.name
    terminal = False
    response_audit: Dict[str, Any] = {"error": str(request_exception), "timestamp": datetime.utcnow().isoformat()}

    if request_exception.response is not None:
        response_audit["response"] = {
            "status": request_exception.response.status_code,
            "body": request_exception.response.text,
        }

    logger.warning(f"{subject} attempt {retry_task.attempts} failed for task: {retry_task.retry_task_id}")

    if retry_task.attempts < max_retries:
        if request_exception.response is None or (500 <= request_exception.response.status_code < 600):
            next_attempt_time = enqueue_retry_task_delay(
                queue=queue,
                connection=connection,
                action=action,
                retry_task=retry_task,
                delay_seconds=pow(backoff_base, float(retry_task.attempts)) * 60,
            )
            logger.info(f"Next attempt time at {next_attempt_time}")
        else:
            terminal = True
            logger.warning(f"Received unhandlable response code ({request_exception.response.status_code}). Stopping")
    else:
        terminal = True
        logger.warning(f"No further retries. Setting status to {RetryTaskStatuses.FAILED}.")
        sentry_sdk.capture_message(
            f"{subject} failed (max attempts reached) for {retry_task}. Stopping... {request_exception}"
        )

    if terminal:
        status = RetryTaskStatuses.FAILED

    return response_audit, status, next_attempt_time


def handle_request_exception(
    db_session: Session,
    *,
    queue: str,
    connection: Any,
    action: Callable,
    backoff_base: int,
    max_retries: int,
    job: rq.job.Job,
    exc_value: Exception,
) -> None:

    response_audit = None
    next_attempt_time = None

    retry_task = get_retry_task(db_session, job.kwargs["retry_task_id"])

    if isinstance(exc_value, requests.RequestException):  # handle http failures specifically
        response_audit, status, next_attempt_time = _handle_request_exception(
            queue, connection, action, backoff_base, max_retries, retry_task, exc_value
        )
    else:  # otherwise report to sentry and fail the task
        status = RetryTaskStatuses.FAILED
        sentry_sdk.capture_exception(exc_value)

    retry_task.update_task(
        db_session,
        next_attempt_time=next_attempt_time,
        response_audit=response_audit,
        status=status,
        clear_next_attempt_time=True,
    )
