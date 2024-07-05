from contextlib import asynccontextmanager
from typing import AsyncGenerator
from urllib.parse import quote_plus

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from src.db.base_class import Base
from src.settings import settings

# Encode the password
encoded_password = quote_plus(settings.POSTGRES_PASSWORD.get_secret_value())

db_url = f"postgresql+asyncpg://{settings.POSTGRES_USER}:{encoded_password}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
engine = create_async_engine(db_url, pool_pre_ping=True, pool_size=5, max_overflow=10)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
)


# Generator for creating a new session
async def get_db_session(autocommit=False) -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal(
        autocommit=autocommit,
    ) as session:
        yield session


# Context manager for creating a new session
get_db_session_cm = asynccontextmanager(get_db_session)


# Function to create tables using the ORM models
async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
