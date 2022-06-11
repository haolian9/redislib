import string

from redislib import SCAN, PairedZSCAN, Redis


async def test_scan(redis: Redis):
    pairs = ((char, char) for char in string.ascii_letters)
    await redis.mset(*pairs)

    scanner = SCAN(redis)
    assert sum([1 async for one in scanner]) == len(string.ascii_letters)


async def test_sscan(redis: Redis):
    pairs = ((ord(char), char) for char in string.ascii_lowercase)
    await redis.zadd('letters', *pairs)

    scanner = PairedZSCAN(redis, 'letters')
    assert set([letter.decode() async for (letter, _) in scanner]) == set(string.ascii_lowercase)
