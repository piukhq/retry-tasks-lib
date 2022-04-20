import logging

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import sentry_sdk

from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import and_, or_

from retry_tasks_lib.db.models import RetryTask, TaskType
from retry_tasks_lib.enums import RetryTaskStatuses

if TYPE_CHECKING:
    from prometheus_client import Gauge

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
