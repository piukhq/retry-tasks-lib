from os import getenv

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()

POSTGRES_DB = getenv("POSTGRES_DB", "retry_tasks_lib_test")
SQLALCHEMY_DATABASE_URI = getenv("SQLALCHEMY_DATABASE_URI", "")
SQL_DEBUG = getenv("SQL_DEBUG", "false").lower() == "true"
REDIS_URL = getenv("REDIS_URL", "")

sync_engine = create_engine(
    SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, echo=SQL_DEBUG, poolclass=NullPool, isolation_level="AUTOCOMMIT"
)
async_engine = create_async_engine(
    SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, echo=SQL_DEBUG, poolclass=NullPool, isolation_level="AUTOCOMMIT"
)
SyncSessionMaker = sessionmaker(sync_engine, expire_on_commit=False)
AsyncSessionMaker = async_sessionmaker(async_engine, expire_on_commit=False)
