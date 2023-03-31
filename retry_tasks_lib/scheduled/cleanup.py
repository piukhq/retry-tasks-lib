from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.sql.expression import delete

from retry_tasks_lib.db.models import RetryTask, RetryTaskStatuses

from . import logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def delete_old_task_data(db_session: "Session", time_reference: datetime) -> None:
    """
    Delete retry_task data (including related db objects i.e task_type_key_values)
    which are dated before the specified time reference

    The function is intended to be called in a sessionmaker context

    Parameters:

        `db_session`:
        A sqlalchemy Session.

        `time_reference`:
        Time reference used to determine which retry_task qualify for deletion.
    """
    # tasks in a successful terminal state
    deleteable_task_statuses = {
        RetryTaskStatuses.SUCCESS,
        RetryTaskStatuses.CANCELLED,
        RetryTaskStatuses.REQUEUED,
        RetryTaskStatuses.CLEANUP,
    }

    logger.info("Cleaning up tasks created before %s...", time_reference.date())
    result = db_session.execute(
        delete(RetryTask)
        .where(
            RetryTask.status.in_(deleteable_task_statuses),
            RetryTask.created_at < time_reference,
        )
        .returning(RetryTask.retry_task_id)
    )
    db_session.commit()

    logger.info("Deleted %d tasks. ( °╭ ︿ ╮°)", len(result.all()))
