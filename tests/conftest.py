import pytest

from redislib import Redis


@pytest.fixture
async def redis() -> Redis:
    async with Redis.from_addr("127.0.0.1", 6379, pool_size=3) as rds:
        try:
            yield rds
        finally:
            await rds.flushdb()
