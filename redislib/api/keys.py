from typing import Optional

from ..typing import RoundTrip, String


class KeysMixin(RoundTrip):
    async def copy(
        self,
        source: String,
        dest: String,
        dest_db: int = None,
        replace: bool = False,
    ) -> int:
        """
        :return: either 0 or 1

        This command copies the value stored at the source key to the
        destination key.

        By default, the destination key is created in the logical database
        used by the connection. The DB option allows specifying an alternative
        logical database index for the destination key.

        The command returns an error when the destination key already exists.
        The REPLACE option removes the destination key before copying the
        value to it."""

        def _args():
            yield source
            yield dest

            if dest_db is not None:
                yield "DB"
                yield dest_db
            if replace:
                yield "REPLACE"

        return await self._round_trip(b"COPY", *_args())

    async def del_(self, *keys: String) -> int:
        """
        :return: number of successful deletion

        Removes the specified keys. A key is ignored if it does not exist."""

        return await self._round_trip(b"DEL", *keys)

    async def dump(self, key: String) -> bytes:
        """Serialize the value stored at key in a Redis-specific format and
        return it to the user. The returned value can be synthesized back into
        a Redis key using the RESTORE command.

        The serialization format is opaque and non-standard, however it has
        a few semantic characteristics:

        It contains a 64-bit checksum that is used to make sure errors will be
        detected. The RESTORE command makes sure to check the checksum before
        synthesizing a key using the serialized value. Values are encoded in
        the same format used by RDB. An RDB version is encoded inside the
        serialized value, so that different Redis versions with incompatible
        RDB formats will refuse to process the serialized value. The
        serialized value does NOT contain expire information. In order to
        capture the time to live of the current value the PTTL command should
        be used.

        If key does not exist a nil bulk reply is returned."""

        return await self._round_trip(b"DUMP", key)

    async def exists(self, *keys: String) -> int:
        """
        Returns if key exists.

        The user should be aware that if the same existing key is mentioned in
        the arguments multiple times, it will be counted multiple times. So if
        somekey exists, EXISTS somekey somekey will return 2.
        """
        return await self._round_trip(b"EXISTS", *keys)

    async def expire(
        self,
        key: String,
        seconds: int,
        nx=False,
        xx=False,
        gt=False,
        lt=False,
    ) -> int:
        """
        :return: either 0 or 1

        Set a timeout on key. After the timeout has expired, the key will
        automatically be deleted. A key with an associated timeout is often
        said to be volatile in Redis terminology.

        The timeout will only be cleared by commands that delete or overwrite
        the contents of the key, including DEL, SET, GETSET and all the *STORE
        commands. This means that all the operations that conceptually alter
        the value stored at the key without replacing it with a new one will
        leave the timeout untouched. For instance, incrementing the value of
        a key with INCR, pushing a new value into a list with LPUSH, or
        altering the field value of a hash with HSET are all operations that
        will leave the timeout untouched.

        The timeout can also be cleared, turning the key back into
        a persistent key, using the PERSIST command.

        If a key is renamed with RENAME, the associated time to live is
        transferred to the new key name.

        If a key is overwritten by RENAME, like in the case of an existing key
        Key_A that is overwritten by a call like RENAME Key_B Key_A, it does
        not matter if the original Key_A had a timeout associated or not, the
        new key Key_A will inherit all the characteristics of Key_B.

        Note that calling EXPIRE/PEXPIRE with a non-positive timeout or
        EXPIREAT/PEXPIREAT with a time in the past will result in the key
        being deleted rather than expired (accordingly, the emitted key event
                will be del, not expired).

        Options

        The EXPIRE command supports a set of options:
        * NX -- Set expiry only when the key has no expiry
        * XX -- Set expiry only when the key has an existing expiry
        * GT -- Set expiry only when the new expiry is greater than current one
        * LT -- Set expiry only when the new expiry is less than current one

        A non-volatile key is treated as an infinite TTL for the purpose of GT
        and LT. The GT, LT and NX options are mutually exclusive.

        Refreshing expires

        It is possible to call EXPIRE using as argument a key that already has
        an existing expire set. In this case the time to live of a key is
        updated to the new value. There are many useful applications for this,
        an example is documented in the Navigation session pattern section
        below.

        Differences in Redis prior 2.1.3

        In Redis versions prior 2.1.3 altering a key with an expire set using
        a command altering its value had the effect of removing the key
        entirely. This semantics was needed because of limitations in the
        replication layer that are now fixed.

        EXPIRE would return 0 and not alter the timeout for a key with a timeout set."""

        def _args():
            yield key
            yield seconds

            if nx:
                yield "NX"
            if xx:
                yield "XX"
            if gt:
                yield "gt"
            if lt:
                yield "lt"

        return await self._round_trip(b"EXPIRE", *_args())

    async def expireat(
        self,
        key: String,
        timestamp: int,
        nx=False,
        xx=False,
        gt=False,
        lt=False,
    ) -> int:
        """
        :return: either 0 or 1

        EXPIREAT has the same effect and semantic as EXPIRE, but instead of
        specifying the number of seconds representing the TTL (time to live),
        it takes an absolute Unix timestamp (seconds since January 1, 1970).
        A timestamp in the past will delete the key immediately."""

        def _args():
            yield key
            yield timestamp

            if nx:
                yield "NX"
            if xx:
                yield "XX"
            if gt:
                yield "gt"
            if lt:
                yield "lt"

        return await self._round_trip(b"EXPIREAT", *_args())

    async def move(self, key: String, db: int) -> int:
        """
        :return: either 0 or 1

        Move key from the currently selected database (see SELECT) to the
        specified destination database. When key already exists in the
        destination database, or it does not exist in the source database, it
        does nothing. It is possible to use MOVE as a locking primitive
        because of this."""

        return await self._round_trip(b"MOVE", key, db)

    async def persist(self, key) -> int:
        """
        :return: either 0 or 1

        Remove the existing timeout on key, turning the key from volatile
        (a key with an expire set) to persistent (a key that will never expire
                as no timeout is associated)."""

        return await self._round_trip(b"PERSIST", key)

    async def randomkey(self) -> Optional[bytes]:
        """Return a random key from the currently selected database."""

        return await self._round_trip(b"RANDOMKEY")

    async def rename(self, key: String, newkey: String) -> bytes:
        """
        :return: b'OK'
        :raise: RedisError('no such key') or any other

        Renames key to newkey. It returns an error when key does not exist.
        If newkey already exists it is overwritten, when this happens RENAME
        executes an implicit DEL operation, so if the deleted key contains
        a very big value it may cause high latency even if RENAME itself is
        usually a constant-time operation.

        In Cluster mode, both key and newkey must be in the same hash slot,
        meaning that in practice only keys that have the same hash tag can be
        reliably renamed in cluster."""

        return await self._round_trip(b"RENAME", key, newkey)

    async def renamenx(self, key: String, newkey: String) -> int:
        """
        :return: either 0 or 1

        Renames key to newkey if newkey does not yet exist. It returns an
        error when key does not exist.

        In Cluster mode, both key and newkey must be in the same hash slot,
        meaning that in practice only keys that have the same hash tag can be
        reliably renamed in cluster."""

        return await self._round_trip(b"RENAMENX", key, newkey)

    async def scan(
        self,
        cursor: int,
        pattern: String = None,
        count: int = None,
        type: String = None,
    ) -> tuple[bytes, list[bytes]]:
        """
        :return: (cursor, [key])

        see api.Scanner
        """

        def _args():
            yield cursor

            if pattern:
                yield "MATCH"
                yield pattern
            if count is not None:
                yield "COUNT"
                yield count
            if type is not None:
                yield "TYPE"
                yield type

        return await self._round_trip(b"SCAN", *_args())

    async def ttl(self, key: String):
        """Returns the remaining time to live of a key that has a timeout.
        This introspection capability allows a Redis client to check how many
        seconds a given key will continue to be part of the dataset.

        In Redis 2.6 or older the command returns -1 if the key does not exist
        or if the key exist but has no associated expire.

        Starting with Redis 2.8 the return value in case of error changed:
        * The command returns -2 if the key does not exist.
        * The command returns -1 if the key exists but has no associated expire."""

        return await self._round_trip(b"TTL", key)

    async def type(self, key: String):
        """Returns the string representation of the type of the value stored
        at key. The different types that can be returned are: Stringing, list,
        set, zset, hash and stream."""

        return await self._round_trip(b"TYPE", key)

    async def unlink(self, *keys: String):
        """This command is very similar to DEL: it removes the specified keys.
        Just like DEL a key is ignored if it does not exist. However the
        command performs the actual memory reclaiming in a different thread,
        so it is not blocking, while DEL is. This is where the command name
        comes from: the command just unlinks the keys from the keyspace. The
        actual removal will happen later asynchronously."""

        return await self._round_trip(b"UNLINK", *keys)
