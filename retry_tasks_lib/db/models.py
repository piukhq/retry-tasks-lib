from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Tuple

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import declarative_base, declarative_mixin, relationship
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.sql.schema import MetaData
from sqlalchemy.sql.sqltypes import String

from retry_tasks_lib.db.retry_query import sync_run_query
from retry_tasks_lib.enums import RetryTaskStatuses, TaskParamsKeyTypes

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

TmpBase: Any = declarative_base()
utc_timestamp_sql = text("TIMEZONE('utc', CURRENT_TIMESTAMP)")


@declarative_mixin
class TimestampMixin:
    created_at = Column(DateTime, server_default=utc_timestamp_sql, nullable=False)
    updated_at = Column(
        DateTime,
        server_default=utc_timestamp_sql,
        onupdate=utc_timestamp_sql,
        nullable=False,
    )


class RetryTask(TmpBase, TimestampMixin):
    __tablename__ = "retry_task"

    retry_task_id = Column(Integer, primary_key=True)
    attempts = Column(Integer, default=0, nullable=False)
    audit_data = Column(MutableList.as_mutable(JSONB), nullable=False, default=text("'[]'::jsonb"))
    next_attempt_time = Column(DateTime, nullable=True)
    status = Column(Enum(RetryTaskStatuses), nullable=False, default=RetryTaskStatuses.PENDING)

    task_type_id = Column(Integer, ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type = relationship("TaskType", back_populates="retry_tasks")
    task_type_key_values = relationship("TaskTypeKeyValue", back_populates="retry_task", lazy="joined")

    def get_task_type_key_values(self, values: list[Tuple[int, str]]) -> list["TaskTypeKeyValue"]:
        return [
            TaskTypeKeyValue(retry_task_id=self.retry_task_id, task_type_key_id=key_id, value=value)
            for key_id, value in values
        ]

    def get_params(self) -> dict:
        task_params: dict = {}
        for value in self.task_type_key_values:
            key = value.task_type_key
            task_params[key.name] = key.type.value(value.value)

        return task_params

    def update_task(
        self,
        db_session: "Session",
        *,
        response_audit: dict = None,
        status: RetryTaskStatuses = None,
        next_attempt_time: datetime = None,
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
                self.next_attempt_time = next_attempt_time

            db_session.commit()

        sync_run_query(_query, db_session)

    def __str__(self) -> str:
        return f"RetryTask PK: {self.retry_task_id} ({self.task_type.name})"


class TaskType(TmpBase, TimestampMixin):
    __tablename__ = "task_type"

    task_type_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, index=True, unique=True)
    path = Column(String, nullable=False)

    retry_tasks = relationship("RetryTask", back_populates="task_type")
    task_type_keys = relationship("TaskTypeKey", back_populates="task_type", lazy="joined")

    def get_key_ids_by_name(self) -> Dict[str, int]:
        return {key.name: key.task_type_key_id for key in self.task_type_keys}

    def __str__(self) -> str:
        return f"{self.name} (pk={self.task_type_id})"


class TaskTypeKey(TmpBase, TimestampMixin):
    __tablename__ = "task_type_key"

    task_type_key_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(Enum(TaskParamsKeyTypes), nullable=False, default=TaskParamsKeyTypes.STRING)

    task_type_id = Column(Integer, ForeignKey("task_type.task_type_id", ondelete="CASCADE"), nullable=False)

    task_type = relationship("TaskType", back_populates="task_type_keys")
    task_type_key_values = relationship("TaskTypeKeyValue", back_populates="task_type_key")

    __table_args__ = (UniqueConstraint("name", "task_type_id", name="name_task_type_id_unq"),)

    def __str__(self) -> str:
        return f"{self.name} (pk={self.task_type_key_id})"


class TaskTypeKeyValue(TmpBase, TimestampMixin):
    __tablename__ = "task_type_key_value"

    value = Column(String, nullable=True)

    retry_task_id = Column(
        Integer,
        ForeignKey("retry_task.retry_task_id", ondelete="CASCADE"),
        primary_key=True,
    )
    task_type_key_id = Column(
        Integer,
        ForeignKey("task_type_key.task_type_key_id", ondelete="CASCADE"),
        primary_key=True,
    )

    task_type_key = relationship("TaskTypeKey", back_populates="task_type_key_values", lazy="joined")
    retry_task = relationship("RetryTask", back_populates="task_type_key_values", lazy="joined")

    def __str__(self) -> str:
        return f"{self.task_type_key.name}: {self.value} (pk={self.retry_task_id},{self.task_type_key_id})"


def load_models_to_metadata(metadata: MetaData) -> None:
    for table in TmpBase.metadata.tables.values():
        table.to_metadata(metadata)
