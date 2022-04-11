import importlib

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import requests
import rq
import sentry_sdk

from sqlalchemy.orm.session import Session

from retry_tasks_lib import logger
from retry_tasks_lib.db.models import RetryTask
from retry_tasks_lib.enums import RetryTaskStatuses
from retry_tasks_lib.utils.synchronous import enqueue_retry_task_delay, get_retry_task

if TYPE_CHECKING:  # pragma: no cover
    from inspect import Traceback


def _handle_request_exception(
    *,
    connection: Any,
    backoff_base: int,
    max_retries: int,
    retry_task: RetryTask,
    request_exception: requests.RequestException,
    extra_status_codes_to_retry: list[int],
) -> tuple[dict, RetryTaskStatuses | None, datetime | None]:
    status = None
    next_attempt_time = None
    subject = retry_task.task_type.name
    terminal = False
    response_audit: dict[str, Any] = {
        "error": str(request_exception),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }

    if request_exception.response is not None:
        response_audit["response"] = {
            "status": request_exception.response.status_code,
            "body": request_exception.response.text,
        }

    logger.warning(f"{subject} attempt {retry_task.attempts} failed for task: {retry_task.retry_task_id}")

    if retry_task.attempts < max_retries:
        resp = request_exception.response
        if resp is None or 500 <= resp.status_code < 600 or resp.status_code in extra_status_codes_to_retry:
            next_attempt_time = enqueue_retry_task_delay(
                connection=connection,
                retry_task=retry_task,
                delay_seconds=pow(backoff_base, float(retry_task.attempts)) * 60,
            )
            status = RetryTaskStatuses.RETRYING
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
    connection: Any,
    backoff_base: int,
    max_retries: int,
    job: rq.job.Job,
    exc_value: Exception,
    extra_status_codes_to_retry: list[int] | None = None,
) -> None:

    response_audit = None
    next_attempt_time = None

    retry_task = get_retry_task(db_session, job.kwargs["retry_task_id"])

    if isinstance(exc_value, requests.RequestException):  # handle http failures specifically
        response_audit, status, next_attempt_time = _handle_request_exception(
            connection=connection,
            backoff_base=backoff_base,
            max_retries=max_retries,
            retry_task=retry_task,
            request_exception=exc_value,
            extra_status_codes_to_retry=extra_status_codes_to_retry or [],
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


def job_meta_handler(job: rq.job.Job, exc_type: type, exc_value: Exception, traceback: "Traceback") -> bool:
    """Resolves any error handler stored in job.meta.

    Falls back to the default RQ error handler (unless worker
    disable_default_exception_handler flag is set)"""
    if error_handler_path := job.meta.get("error_handler_path"):
        try:
            mod, func = error_handler_path.rsplit(".", 1)
            mod = importlib.import_module(mod)
            handler = getattr(mod, func)
            return handler(job, exc_type, exc_value, traceback)
        except (ValueError, ModuleNotFoundError, AttributeError) as ex:
            logger.warning(f"Could not import error handler for job {job} (meta={job.meta}): {ex}")
    return True
