from datetime import date, datetime
from enum import Enum
from typing import Any


def to_bool(v: str) -> bool:
    if v.lower() in ["true", "1", "t", "yes", "y"]:
        return True
    else:
        return False


class RetryTaskStatuses(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    RETRYING = "retrying"
    FAILED = "failed"
    SUCCESS = "success"
    WAITING = "waiting"
    CANCELLED = "cancelled"
    REQUEUED = "requeued"


class TaskParamsKeyTypes(Enum):
    STRING = str
    INTEGER = int
    FLOAT = float
    BOOLEAN = bool
    DATE = date
    DATETIME = datetime

    def convert_value(self, v: str) -> Any:
        if self.value == bool:
            return v.lower() in ["true", "1", "t", "yes", "y"]

        elif self.value in [date, datetime]:
            return self.value.fromisoformat(v)

        else:
            return self.value(v)
