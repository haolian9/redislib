import logging
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Awaitable, Callable

import attr
import trio
from respy3 import Resp3Reader
from trio import BrokenResourceError, Condition, Event, Lock, SocketStream

from .typing import Arg, Cmd

_log = logging.getLogger(__name__)


def pack_cmd(cmd: Cmd, *args: Arg):
    """
    re-implemented respy3.write_command() with less bytes allocation
    """

    def _parts():
        cmd_bin = cmd
        yield "*{}".format(len(args) + 1).encode()
        yield "${}".format(len(cmd_bin)).encode()
        yield cmd_bin

        for arg in args:
            if isinstance(arg, (int, float)):
                arg_bin = str(arg).encode()
            elif isinstance(arg, str):
                arg_bin = arg.encode("utf-8")
            elif isinstance(arg, (bytes, bytearray)):
                arg_bin = arg
            else:
                raise RuntimeError(f"unknown type for arg: {arg!r}")
            yield "${}".format(len(arg_bin)).encode()
            yield arg_bin

        yield b""  # for trailing \r\n

    return b"\r\n".join(_parts())


class ClosedPoolError(Exception):
    ...


@attr.s
class Hello:
    version = 3
    username: str = attr.ib(default=None)
    password: str = attr.ib(default=None)
    clientname: str = attr.ib(default=None)

    def as_args(self):
        yield 3

        if self.username:
            yield "AUTH"
            yield self.username
            yield self.password

        if self.clientname:
            yield "SETNAME"
            yield self.clientname


@attr.s(hash=True)
class Connection:
    _sock: SocketStream = attr.ib()
    # TODO@haoliang since we will re-assign self.round_trip, _said_hello is really needed?
    _said_hello: bool = attr.ib(default=False)
    _protocol: Resp3Reader = attr.ib(init=False, factory=Resp3Reader)

    async def hello(self, hi: Hello):
        if self._said_hello:
            return

        reply = await self._round_trip(b"HELLO", *hi.as_args())
        self._said_hello = True

        self.round_trip = self._round_trip  # type: ignore

        return reply

    async def _round_trip(self, cmd: Cmd, *args: Arg):
        protocol = self._protocol
        packed = pack_cmd(cmd, *args)
        sock = self._sock

        await sock.send_all(packed)

        while True:
            bite = await sock.receive_some()

            if bite == b"":
                raise BrokenResourceError

            protocol.feed(bite)

            result = protocol.get_object()

            if result is not protocol.sentinel:
                break

        assert not (
            protocol.new_state_stack and protocol.state_stack
        ), "protocol in dirty state"

        return result

    async def round_trip(self, cmd: Cmd, *args: Arg):
        assert not self._said_hello
        raise RuntimeError("need to say hello to redis first")

    @classmethod
    async def from_addr(cls, host: str, port: int):
        sock = await trio.open_tcp_stream(host, port)

        return cls(sock=sock)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exctype, exc, tb):
        # TODO@haoliang send a quit() call?
        try:
            await self._sock.send_eof()
        finally:
            await self._sock.aclose()


@attr.s
class Pool:
    """
    we choose AsyncExitStack to close all connections safetly:
    * CON: there is no hijack() that user can choose not to release

    when pool has been closed, acquire() will raise;
    also release(), that's said, user should ensure each acquired conn had
    been released to pool before close.
    """

    _factory: Callable[..., Awaitable["Connection"]] = attr.ib()
    _max_num: int = attr.ib()

    _lock: Lock = attr.ib(init=False, factory=Lock)
    _one_available: Condition = attr.ib(init=False, factory=Condition)
    _acquired_num: int = attr.ib(init=False, default=0)
    _conns: set[Connection] = attr.ib(init=False, factory=set)
    _closed: Event = attr.ib(init=False, factory=Event)
    _exitstack: AsyncExitStack = attr.ib(init=False, factory=AsyncExitStack)

    async def acquire(self):
        if self._closed.is_set():
            raise ClosedPoolError

        async with self._lock:
            # take an idle conn
            try:
                conn = self._conns.pop()
            except KeyError:
                pass
            else:
                _log.debug("acquired idle %s", conn)
                self._acquired_num += 1
                return conn

            # or, try to create one
            if self._max_num > self._acquired_num:
                conn = await self._factory()
                _log.debug("acquired new %s", conn)
                await self._exitstack.enter_async_context(conn)
                self._acquired_num += 1
                return conn

        # or wait for someone releases one
        async with self._one_available:
            _log.debug("full pool, waiting for release")
            await self._one_available.wait()

        async with self._lock:
            conn = self._conns.pop()
            _log.debug("acquired released %s", conn)
            self._acquired_num += 1
            return conn

    @asynccontextmanager
    async def acquire_then_release(self):
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)

    async def release(self, conn: "Connection"):
        if self._closed.is_set():
            raise ClosedPoolError

        async with self._lock:
            if conn in self._conns:
                return

            _log.debug("released %s", conn)
            self._conns.add(conn)
            self._acquired_num -= 1

            async with self._one_available:
                self._one_available.notify(1)

    async def __aenter__(self):
        return self

    async def aclose(self):
        if self._closed.is_set():
            return

        assert len(self._exitstack._exit_callbacks) == len(
            self._conns
        ), "incompleted release: {} vs. {}".format(
            len(self._exitstack._exit_callbacks), len(self._conns)
        )

        self._closed.set()
        await self._exitstack.aclose()

    async def __aexit__(self, exctype, exc, tb):
        await self.aclose()
