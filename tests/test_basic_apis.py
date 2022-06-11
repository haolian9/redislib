from redislib import Redis


async def test_ping(redis: Redis):
    assert await redis.ping() == b'PONG'


async def test_mset(redis: Redis):
    pairs = ((str(i), str(i)) for i in range(100))
    assert await redis.mset(*pairs) == b'OK'
