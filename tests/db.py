from os import getenv

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()

POSTGRES_DB = getenv("POSTGRES_DB", "retry_tasks_lib_test")
SQLALCHEMY_DATABASE_URI = getenv("SQLALCHEMY_DATABASE_URI", "")
SQL_DEBUG = getenv("SQL_DEBUG", "false").lower() == "true"
REDIS_URL = getenv("REDIS_URL", "")

async_engine = create_async_engine(
    SQLALCHEMY_DATABASE_URI, pool_pre_ping=True, future=True, echo=SQL_DEBUG, poolclass=NullPool
)
sync_engine = create_engine(
    SQLALCHEMY_DATABASE_URI.replace("+asyncpg", ""), pool_pre_ping=True, poolclass=NullPool, echo=SQL_DEBUG, future=True
)
AsyncSessionMaker = sessionmaker(bind=async_engine, future=True, expire_on_commit=False, class_=AsyncSession)
SyncSessionMaker = sessionmaker(bind=sync_engine, future=True, expire_on_commit=False)
