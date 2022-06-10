from ..typing import RoundTrip


class TransactionsMixin(RoundTrip):
    async def exec(self):
        """Executes all previously queued commands in a transaction and
        restores the connection state to normal.

        When using WATCH, EXEC will execute commands only if the watched keys
        were not modified, allowing for a check-and-set mechanism."""

        return await self._round_trip(b"EXEC")

    async def discard(self):
        """Flushes all previously queued commands in a transaction and
        restores the connection state to normal.

        If WATCH was used, DISCARD unwatches all keys watched by the
        connection."""

        return await self._round_trip(b"DISCARD")

    async def multi(self):
        """Marks the start of a transaction block. Subsequent commands will
        be queued for atomic execution using EXEC."""

        return await self._round_trip(b"MULTI")

    async def watch(self, *keys: str):
        """Marks the given keys to be watched for conditional execution of
        a transaction."""

        return await self._round_trip(b"WATCH", *keys)

    async def unwatch(self):
        """Flushes all the previously watched keys for a transaction.
        If you call EXEC or DISCARD, there's no need to manually call UNWATCH."""

        return await self._round_trip(b"UNWATCH")
