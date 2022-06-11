import logging
from contextlib import asynccontextmanager

import attrs
from respy3 import Resp3Reader

from ..connection import Connection, Hello, Pool
from ..typing import Arg, Cmd
from .connection import ConnectionMixin
from .hashes import HashesMixin
from .keys import KeysMixin
from .lists import ListsMixin
from .server import ServerMixin
from .strings import StringsMixin
from .transactions import TransactionsMixin
from .zsets import ZsetsMixin

_log = logging.getLogger(__name__)


@attrs.define
class Redis(
    KeysMixin,
    StringsMixin,
    ListsMixin,
    ZsetsMixin,
    ConnectionMixin,
    ServerMixin,
    HashesMixin,
):
    _pool: Pool
    _protocol: Resp3Reader = attrs.field(init=False, factory=Resp3Reader)

    async def _round_trip(self, cmd: Cmd, *args: Arg):
        async with self._pool.acquire_then_release() as conn:
            return await conn.round_trip(cmd, *args)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exctype, exc, tb):
        await self._pool.aclose()

    @asynccontextmanager
    async def open_transaction(self):
        """
        since Transaction can be used multi times, there is no need to multi() in here.
        just let users do what they want.
        """
        async with self._pool.acquire_then_release() as conn:
            with Transaction(conn) as tx:
                yield tx

    @classmethod
    def from_addr(
        cls, host: str, port: int, pool_size: int = 20, hello: Hello = Hello()
    ):
        async def _conn_factory():
            conn = await Connection.from_addr(host, port)
            reply = await conn.hello(hello)

            _log.debug("hello: %s", reply)

            return conn

        pool = Pool(factory=_conn_factory, max_num=pool_size)

        return cls(pool=pool)


@attrs.define
class Transaction(
    KeysMixin,
    StringsMixin,
    ListsMixin,
    ZsetsMixin,
    ConnectionMixin,
    ServerMixin,
    HashesMixin,
    TransactionsMixin,
):
    """
    compare to Redis, Transaction extends TransactionsMixin
    """

    _conn: Connection

    _tx_count: int = attrs.field(init=False, default=0)
    """ +1 when multi, -1 when exec/discard """

    async def _round_trip(self, cmd: Cmd, *args: Arg):
        return await self._conn.round_trip(cmd, *args)

    async def multi(self):
        self._tx_count += 1
        return await super().multi()

    async def discard(self):
        self._tx_count -= 1
        return await super().discard()

    async def exec(self):
        self._tx_count -= 1
        return await super().exec()

    def __enter__(self):
        return self

    def __exit__(self, exc, exc_type, tb):
        if self._tx_count != 0:
            raise RuntimeError("unmatched transaction acquiring and releasing")

        # TODO@haoliang need to destory this dirty conn? as user can catch the
        # RuntimeError then continue to use it
        self._conn = None
