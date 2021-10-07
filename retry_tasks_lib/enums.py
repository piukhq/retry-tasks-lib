from datetime import date, datetime
from enum import Enum

from settings import to_bool


class RetryTaskStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"
    CANCELLED = "cancelled"


class TaskParamsKeyTypes(Enum):
    STRING = str
    INTEGER = int
    FLOAT = float
    BOOLEAN = to_bool
    DATE = date.fromisoformat
    DATETIME = datetime.fromisoformat
