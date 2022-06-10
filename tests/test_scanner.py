from redislib import SCAN, Redis


async def test_scan(redis: Redis):
    await redis.mset(*((str(i), i) for i in range(100)))

    scanner = SCAN(redis)

    assert sum([1 async for one in scanner]) == 100
