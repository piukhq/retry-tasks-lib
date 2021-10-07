from datetime import date, datetime
from enum import Enum


class QueuedRetryStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"


class TaskParamsKeyTypes(Enum):
    STRING = str
    INTEGER = int
    FLOAT = float
    DATE = date.fromisoformat
    DATETIME = datetime.fromisoformat
