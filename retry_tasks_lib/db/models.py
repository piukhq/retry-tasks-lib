import json

from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import DeclarativeBase, Mapped, declarative_mixin, mapped_column, relationship
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.schema import MetaData

from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses, TaskParamsKeyTypes

if TYPE_CHECKING:

    from sqlalchemy.orm import Session


class TmpBase(DeclarativeBase):
    pass


utc_timestamp_sql = text("TIMEZONE('utc', CURRENT_TIMESTAMP)")


@declarative_mixin
class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=utc_timestamp_sql, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        server_default=utc_timestamp_sql,
        onupdate=utc_timestamp_sql,
        nullable=False,
    )


class RetryTask(TmpBase, TimestampMixin):
    __tablename__ = "retry_task"

    retry_task_id: Mapped[int] = mapped_column(primary_key=True)
    attempts: Mapped[int] = mapped_column(default=0, nullable=False)
    audit_data: Mapped[list] = mapped_column(
        MutableList.as_mutable(JSONB),  # type: ignore
        nullable=False,
        default=text("'[]'::jsonb"),
    )
    next_attempt_time: Mapped[datetime] = mapped_column(nullable=True)
    status: Mapped[RetryTaskStatuses] = mapped_column(nullable=False, default=RetryTaskStatuses.PENDING, index=True)

    task_type_id: Mapped[int] = mapped_column(ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type: Mapped["TaskType"] = relationship("TaskType", back_populates="retry_tasks")
    task_type_key_values: Mapped[list["TaskTypeKeyValue"]] = relationship(
        "TaskTypeKeyValue", back_populates="retry_task", lazy="joined"
    )

    def get_task_type_key_values(self, values: list[tuple[int, str]]) -> list["TaskTypeKeyValue"]:
        task_type_key_values: list[TaskTypeKeyValue] = []
        for key_id, value in values:
            serialization_fn: Callable[..., str] = (
                getattr(json, "dumps") if isinstance(value, (dict, list)) else str  # noqa: B009, UP038
            )
            val = serialization_fn(value)
            task_type_key_values.append(
                TaskTypeKeyValue(retry_task_id=self.retry_task_id, task_type_key_id=key_id, value=val)
            )
        return task_type_key_values

    def get_params(self) -> dict:
        task_params: dict = {}
        for value in self.task_type_key_values:
            key = value.task_type_key
            task_params[key.name] = key.type.convert_value(value.value)

        return task_params

    def update_task(
        self,
        db_session: "Session",
        *,
        response_audit: dict | None = None,
        status: RetryTaskStatuses | None = None,
        next_attempt_time: datetime | None = None,
        increase_attempts: bool = False,
        clear_next_attempt_time: bool = False,
    ) -> None:
        def _query(db_session: "Session") -> None:
            if response_audit is not None:
                self.audit_data.append(response_audit)
                flag_modified(self, "audit_data")

            if status is not None:
                self.status = status

            if increase_attempts:
                self.attempts += 1

            if clear_next_attempt_time or next_attempt_time is not None:
                self.next_attempt_time = next_attempt_time  # type: ignore

            db_session.commit()

        sync_run_query(_query, db_session)

    def __str__(self) -> str:
        return f"RetryTask PK: {self.retry_task_id} ({self.task_type.name})"


class TaskType(TmpBase, TimestampMixin):
    __tablename__ = "task_type"

    task_type_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False, index=True, unique=True)
    path: Mapped[str] = mapped_column(nullable=False)
    cleanup_handler_path: Mapped[str] = mapped_column(nullable=True)
    error_handler_path: Mapped[str] = mapped_column(nullable=False)
    queue_name: Mapped[str] = mapped_column(nullable=False)

    retry_tasks: Mapped[list["RetryTask"]] = relationship("RetryTask", back_populates="task_type")
    task_type_keys: Mapped[list["TaskTypeKey"]] = relationship("TaskTypeKey", back_populates="task_type", lazy="joined")

    def get_key_ids_by_name(self) -> dict[str, int]:
        return {key.name: key.task_type_key_id for key in self.task_type_keys}

    def __str__(self) -> str:
        return f"{self.name} (pk={self.task_type_id})"


class TaskTypeKey(TmpBase, TimestampMixin):
    __tablename__ = "task_type_key"

    task_type_key_id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[TaskParamsKeyTypes] = mapped_column(nullable=False, default=TaskParamsKeyTypes.STRING)  # noqa: A003

    task_type_id: Mapped[int] = mapped_column(ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type: Mapped["TaskType"] = relationship("TaskType", back_populates="task_type_keys")
    task_type_key_values: Mapped[list["TaskTypeKeyValue"]] = relationship(
        "TaskTypeKeyValue", back_populates="task_type_key"
    )

    __table_args__ = (UniqueConstraint("name", "task_type_id", name="name_task_type_id_unq"),)

    def __str__(self) -> str:
        return f"{self.name} (pk={self.task_type_key_id})"


class TaskTypeKeyValue(TmpBase, TimestampMixin):
    __tablename__ = "task_type_key_value"

    value: Mapped[str] = mapped_column(nullable=True)

    retry_task_id: Mapped[int] = mapped_column(
        ForeignKey("retry_task.retry_task_id", ondelete="CASCADE"), primary_key=True
    )
    task_type_key_id: Mapped[int] = mapped_column(
        ForeignKey("task_type_key.task_type_key_id", ondelete="CASCADE"), primary_key=True
    )

    task_type_key: Mapped["TaskTypeKey"] = relationship(
        "TaskTypeKey", back_populates="task_type_key_values", lazy="joined"
    )
    retry_task: Mapped["RetryTask"] = relationship("RetryTask", back_populates="task_type_key_values", lazy="joined")

    def __str__(self) -> str:
        return f"{self.task_type_key.name}: {self.value} (pk={self.retry_task_id},{self.task_type_key_id})"


def load_models_to_metadata(metadata: MetaData) -> None:
    for table in TmpBase.metadata.tables.values():
        table.to_metadata(metadata)
