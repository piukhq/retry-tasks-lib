import logging

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import sentry_sdk

from rq import Queue
from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import and_, or_

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses

if TYPE_CHECKING:
    from prometheus_client import Gauge
    from redis import Redis

logger = logging.getLogger(__name__)


def report_anomalous_tasks(*, session_maker: sessionmaker, project_name: str, gauge: "Gauge") -> None:
    """
    Query a database to find tasks that need reporting.

    Parameters:
        session_maker: A sessionmaker factory for the database
        project_name:  The name of the project for reporting
        guage:         An instance of prometheus_client.Gauge.
                       The Guage must have labels of ("app", "task_name", "status")
    """
    logger.info(f"Updating {gauge} metrics ...")
    try:
        with session_maker() as db_session:
            now = datetime.now(tz=timezone.utc)
            res = (
                db_session.execute(
                    select(TaskType.name, RetryTask.status, func.count(RetryTask.retry_task_id).label("count"))
                    .join(TaskType)
                    .where(
                        or_(
                            and_(
                                RetryTask.status == RetryTaskStatuses.PENDING,
                                RetryTask.updated_at < now - timedelta(hours=1),
                            ),
                            and_(
                                RetryTask.status == RetryTaskStatuses.IN_PROGRESS,
                                or_(
                                    RetryTask.next_attempt_time.is_(None),
                                    RetryTask.next_attempt_time < now,
                                ),
                                RetryTask.updated_at < now - timedelta(hours=1),
                            ),
                            and_(
                                RetryTask.status == RetryTaskStatuses.WAITING,
                                RetryTask.updated_at < now - timedelta(days=2),
                            ),
                        )
                    )
                    .group_by(TaskType.name, RetryTask.status)
                )
                .mappings()
                .all()
            )
            for row in res:
                gauge.labels(
                    app=project_name,
                    task_name=row["name"],
                    status=RetryTaskStatuses(row["status"]).name,
                ).set(int(row["count"]))

    except Exception as ex:  # pylint: disable=broad-except
        sentry_sdk.capture_exception(ex)


def report_tasks_summary(*, session_maker: sessionmaker, project_name: str, gauge: "Gauge") -> None:
    """
    Query a database to find the current total runs of a task type, and what status they are in

    Parameters:
        session_maker: A sessionmaker factory for the database
        project_name:  The name of the project for reporting
        guage:         An instance of prometheus_client.Gauge.
                       The Guage must have labels of ("app", "task_name", "status")
    """
    logger.info(f"Updating {gauge} metrics ...")
    try:
        with session_maker() as db_session:
            res = (
                db_session.execute(
                    select(TaskType.name, RetryTask.status, func.count(RetryTask.retry_task_id).label("count"))
                    .join(TaskType)
                    .group_by(TaskType.name, RetryTask.status)
                )
                .mappings()
                .all()
            )
            for row in res:
                gauge.labels(
                    app=project_name,
                    task_name=row["name"],
                    status=RetryTaskStatuses(row["status"]).name,
                ).set(int(row["count"]))

    except Exception as ex:  # pylint: disable=broad-except
        sentry_sdk.capture_exception(ex)


def report_queue_lengths(*, redis: "Redis", project_name: str, gauge: "Gauge", queue_names: list[str]) -> None:
    """
    Inspect RQ queues to report queue length

    Parameters:
        redis:        A redis connection instance
        project_name: The name of the project for reporting
        queue_names:  The queue names to report on
        guage:        An instance of prometheus_client.Gauge.
                      The Guage must have labels of ("app", "queue_name")
    """
    logger.info(f"Updating {gauge} metrics ...")
    try:
        for queue_name in queue_names:
            queue = Queue(queue_name, connection=redis)
            gauge.labels(app=project_name, queue_name=queue_name).set(len(queue))
    except Exception as ex:  # pylint: disable=broad-except
        sentry_sdk.capture_exception(ex)
