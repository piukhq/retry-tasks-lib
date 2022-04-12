DB_CONNECTION_RETRY_TIMES = 3
DEFAULT_FAILURE_TTL = 60 * 60 * 24 * 7  # 1 week
USE_NULL_POOL = False


# pylint: disable=global-statement
def load_settings(settings: object) -> None:
    global USE_NULL_POOL
    global DEFAULT_FAILURE_TTL
    global DB_CONNECTION_RETRY_TIMES

    USE_NULL_POOL = getattr(settings, "USE_NULL_POOL", USE_NULL_POOL)
    DEFAULT_FAILURE_TTL = getattr(settings, "DEFAULT_FAILURE_TTL", DEFAULT_FAILURE_TTL)
    DB_CONNECTION_RETRY_TIMES = getattr(settings, "DB_CONNECTION_RETRY_TIMES", DB_CONNECTION_RETRY_TIMES)
