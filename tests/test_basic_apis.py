from redislib import Redis


async def test_ping(redis: Redis):
    assert redis.ping() == b'OK'


async def test_mset(redis: Redis):
    n = await redis.mset(*((str(i), i) for i in range(100)))
    assert n == 100
