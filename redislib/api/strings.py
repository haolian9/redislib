from ..typing import RoundTrip, String


class StringsMixin(RoundTrip):
    async def decr(self, key: String):
        """Decrements the number stored at key by one. If the key does not
        exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains
        a string that can not be represented as integer. This operation is
        limited to 64 bit signed integers."""

        return await self._round_trip(b"DECR", key)

    async def decrby(self, key: String, decrement: int):
        """Decrements the number stored at key by decrement. If the key does
        not exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains
        a string that can not be represented as integer. This operation is
        limited to 64 bit signed integers."""

        return await self._round_trip(b"DECRBY", key, decrement)

    async def get(self, key: String) -> bytes:
        """Get the value of key. If the key does not exist the special value
        nil is returned. An error is returned if the value stored at key is
        not a string, because GET only handles string values."""

        return await self._round_trip(b"GET", key)

    async def getdel(self, key: String):
        """Get the value of key and delete the key. This command is similar to
        GET, except for the fact that it also deletes the key on success (if
        and only if the key's value type is a string)."""

        return await self._round_trip(b"GETDEL", key)

    async def getex(
        self,
        key: String,
        ex: int = None,
        px: int = None,
        exat: int = None,
        pxat: int = None,
        persist: bool = False,
    ):
        """Get the value of key and optionally set its expiration. GETEX is
        similar to GET, but is a write command with additional options.

        Options

        The GETEX command supports a set of options that modify its behavior:
        * EX seconds -- Set the specified expire time, in seconds.
        * PX milliseconds -- Set the specified expire time, in milliseconds.
        * EXAT timestamp-seconds -- Set the specified Unix time at which the key will expire, in seconds.
        * PXAT timestamp-milliseconds -- Set the specified Unix time at which the key will expire, in milliseconds.
        * PERSIST -- Remove the time to live associated with the key.
        """

        def _args():
            yield key

            if ex is not None:
                yield ex
            if px is not None:
                yield px
            if exat is not None:
                yield exat
            if pxat is not None:
                yield pxat
            if persist:
                yield "PERSIST"

        return await self._round_trip(b"GETEX", *_args())

    async def incr(self, key: String):
        """Increments the number stored at key by one. If the key does not
        exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains
        a string that can not be represented as integer. This operation is
        limited to 64 bit signed integers.

        Note: this is a string operation because Redis does not have
        a dedicated integer type. The string stored at the key is interpreted
        as a base-10 64 bit signed integer to execute the operation.

        Redis stores integers in their integer representation, so for string
        values that actually hold an integer, there is no overhead for storing
        the string representation of the integer."""

        return await self._round_trip(b"INCR", key)

    async def incrby(self, key: String, increment: int):
        """Increments the number stored at key by increment. If the key does
        not exist, it is set to 0 before performing the operation. An error is
        returned if the key contains a value of the wrong type or contains
        a string that can not be represented as integer. This operation is
        limited to 64 bit signed integers."""

        return await self._round_trip(b"INCRBY", key, increment)

    async def mget(self, *keys: String):
        """Returns the values of all specified keys. For every key that does
        not hold a string value or does not exist, the special value nil is
        returned. Because of this, the operation never fails."""

        return await self._round_trip(b"MGET", *keys)

    async def mset(self, *key_value: tuple[str, str]):
        """Sets the given keys to their respective values. MSET replaces
        existing values with new values, just as regular SET. See MSETNX if
        you don't want to overwrite existing values.

        MSET is atomic, so all given keys are set at once. It is not possible
        for clients to see that some of the keys were updated while others are
        unchanged."""

        def _args():
            for key, value in key_value:
                yield key
                yield value

        return await self._round_trip(b"MSET", *_args())

    async def msetnx(self, *key_value: tuple[str, str]):
        """Sets the given keys to their respective values. MSETNX will not
        perform any operation at all even if just a single key already exists.

        Because of this semantic MSETNX can be used in order to set different
        keys representing different fields of an unique logic object in a way
        that ensures that either all the fields or none at all are set.

        MSETNX is atomic, so all given keys are set at once. It is not
        possible for clients to see that some of the keys were updated while
        others are unchanged."""

        def _args():
            for key, value in key_value:
                yield key
                yield value

        return await self._round_trip(b"MSETNX", *_args())

    async def set(
        self,
        key: String,
        value: String,
        ex: int = None,
        px: int = None,
        exat: int = None,
        pxat: int = None,
        keepttl: bool = False,
        nx: bool = False,
        xx: bool = False,
        get: bool = False,
    ):
        """Set key to hold the string value. If key already holds a value, it
        is overwritten, regardless of its type. Any previous time to live
        associated with the key is discarded on successful SET operation.

        Options

        The SET command supports a set of options that modify its behavior:
        * EX seconds -- Set the specified expire time, in seconds.
        * PX milliseconds -- Set the specified expire time, in milliseconds.
        * EXAT timestamp-seconds -- Set the specified Unix time at which the
            key will expire, in seconds.
        * PXAT timestamp-milliseconds -- Set the specified Unix time at which
            the key will expire, in milliseconds.
        * NX -- Only set the key if it does not already exist.
        * XX -- Only set the key if it already exist.
        * KEEPTTL -- Retain the time to live associated with the key.
        * GET -- Return the old string stored at key, or nil if key did not
            exist. An error is returned and SET aborted if the value stored at key
            is not a string.

        Note: Since the SET command options can replace SETNX, SETEX, PSETEX,
        GETSET, it is possible that in future versions of Redis these commands
        will be deprecated and finally removed."""

        def _args():
            yield key
            yield value

            if ex is not None:
                yield ex
            if px is not None:
                yield px
            if exat is not None:
                yield exat
            if pxat is not None:
                yield pxat
            if keepttl:
                yield "KEEPTTL"
            if nx:
                yield "NX"
            if xx:
                yield "XX"
            if get:
                yield "GET"

        return await self._round_trip(b"SET", *_args())

    async def setnx(self, key: String, value: String):
        """Set key to hold string value if key does not exist. In that case,
        it is equal to SET. When key already holds a value, no operation is
        performed. SETNX is short for "SET if Not eXists"."""

        return await self._round_trip(b"SETNX", key, value)
