from datetime import date, datetime
from enum import Enum


def to_bool(v: str) -> bool:
    if v.lower() in ["true", "1", "t", "yes", "y"]:
        return True
    else:
        return False


class RetryTaskStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"
    CANCELLED = "cancelled"
    REQUEUED = "requeued"


class TaskParamsKeyTypes(Enum):
    STRING = str
    INTEGER = int
    FLOAT = float
    BOOLEAN = to_bool
    DATE = date.fromisoformat
    DATETIME = datetime.fromisoformat
