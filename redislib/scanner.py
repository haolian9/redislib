"""
notes:
* scan abort:
    * free to terminate
    * https://redis.io/commands/scan#terminating-iterations-in-the-middle
* count has no effects sometime:
    * it is a known behavior
    * https://redis.io/commands/scan#why-scan-may-return-all-the-items-of-an-aggregate-data-type-in-a-single-call
"""

import attr

from .api.redis import Redis
from .typing import String


@attr.s
class Scanner:
    """https://redis.io/commands/scan"""

    _state: str = attr.ib(init=False, default="init")
    """ init -> started -> scanned -> stopped """

    _cursor: bytes = attr.ib(init=False, default=b"0")
    _stash: list = attr.ib(init=False, factory=list)

    def __aiter__(self):
        if self._state != "init":
            raise RuntimeError("illegal state to start iter")

        self._state = "started"
        return self

    async def _scan(self):
        raise NotImplementedError

    async def __anext__(self) -> bytes:
        async def _next():
            try:
                return self._stash.pop()
            except IndexError:
                if self._state == "scanned":
                    self._state = "stopped"
                    assert self._cursor == b"0"
                    raise StopAsyncIteration

                cursor, stash = await self._scan()

                self._cursor = cursor
                self._stash = stash

                if cursor == b"0":
                    self._state = "scanned"

                    if not stash:
                        self._state = "stopped"
                        raise StopAsyncIteration

                # scan may return zero stash
                return self._stash.pop()

        if self._state == "stopped":
            raise StopAsyncIteration

        if self._state not in ("started", "scanned"):
            raise RuntimeError("illegal state to next")

        while True:
            try:
                return await _next()
            except IndexError:
                pass


@attr.s
class ZSCAN(Scanner):
    """
    yield score
    yield member
    ...
    """

    _redis: Redis = attr.ib()
    _key: String = attr.ib()
    _pattern: String = attr.ib(default=None)
    # TODO@haoliang dynamic count
    _count: int = attr.ib(default=None)

    async def _scan(self):
        return await self._redis.zscan(
            self._key, self._cursor, pattern=self._pattern, count=self._count
        )


@attr.s
class SCAN(Scanner):
    """
    yield member
    ...
    """

    _redis: Redis = attr.ib()
    _pattern: String = attr.ib(default=None)
    # TODO@haoliang dynamic count
    _count: int = attr.ib(default=None)
    _type: String = attr.ib(default=None)

    async def _scan(self):
        return await self._redis.scan(
            self._cursor, pattern=self._pattern, count=self._count, type=self._type
        )


class HSCAN(Scanner):
    """
    yield field
    yield value
    ...
    """

    _redis: Redis = attr.ib()
    _key: String = attr.ib()
    _pattern: String = attr.ib(default=None)
    # TODO@haoliang dynamic count
    _count: int = attr.ib(default=None)

    async def _scan(self):
        return await self._redis.hscan(
            self._key, self._cursor, pattern=self._pattern, count=self._count
        )


class SSCAN(Scanner):
    """
    yield member
    ...
    """

    _redis: Redis = attr.ib()
    _key: String = attr.ib()
    _pattern: String = attr.ib(default=None)
    # TODO@haoliang dynamic count
    _count: int = attr.ib(default=None)

    async def _scan(self):
        return await self._redis.sscan(
            self._key, self._cursor, pattern=self._pattern, count=self._count
        )


class PairedZSCAN(ZSCAN):
    """yield (member, score)"""

    async def __anext__(self) -> tuple[bytes, float]:  # type: ignore
        super_anext = super().__anext__

        async def _next():
            score = await super_anext()
            member = await super_anext()

            return member, float(score)

        return await _next()
