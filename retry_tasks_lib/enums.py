import enum
import json

from datetime import date, datetime


class RetryTaskStatuses(enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    RETRYING = "retrying"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"
    CANCELLED = "cancelled"
    REQUEUED = "requeued"
    CLEANUP = "cleanup"
    CLEANUP_FAILED = "cleanupfailed"

    @classmethod
    def cancellable_statuses_names(cls) -> list[str]:
        return [
            task_status.name
            for task_status in cls
            if task_status not in (cls.SUCCESS, cls.CANCELLED, cls.REQUEUED, cls.IN_PROGRESS)
        ]

    @classmethod
    def requeueable_statuses_names(cls) -> list[str]:
        return [cls.FAILED.name, cls.CANCELLED.name]

    @classmethod
    def enqueuable_statuses(cls) -> list:
        return [
            cls.PENDING,
            cls.WAITING,
            cls.CLEANUP,
        ]


class TaskParamsKeyTypes(enum.Enum):
    STRING = enum.auto()
    INTEGER = enum.auto()
    FLOAT = enum.auto()
    BOOLEAN = enum.auto()
    DATE = enum.auto()
    DATETIME = enum.auto()
    JSON = enum.auto()

    def convert_value(self, v: str) -> str | int | float | bool | date | datetime | bytes | bytearray:  # noqa: PLR0911
        match self:
            case TaskParamsKeyTypes.STRING:
                return v
            case TaskParamsKeyTypes.INTEGER:
                return int(v)
            case TaskParamsKeyTypes.FLOAT:
                return float(v)
            case TaskParamsKeyTypes.BOOLEAN:
                return v.lower() in {"true", "1", "t", "yes", "y"}
            case TaskParamsKeyTypes.DATE:
                return date.fromisoformat(v)
            case TaskParamsKeyTypes.DATETIME:
                return datetime.fromisoformat(v)
            case TaskParamsKeyTypes.JSON:
                return json.loads(v)
