from ..typing import RoundTrip, String


class HashesMixin(RoundTrip):
    """https://redis.io/commands#hash"""

    async def hdel(self, key: String, *fields: String) -> int:
        """
        :return: number of successful deletion

        Removes the specified fields from the hash stored at key. Specified
        fields that do not exist within this hash are ignored. If key does not
        exist, it is treated as an empty hash and this command returns 0."""

        return await self._round_trip(b"HDEL", key, *fields)

    async def hexists(self, key: String, field: String) -> int:
        """
        :return: either 0 or 1

        Returns if field is an existing field in the hash stored at key."""

        return await self._round_trip(b"HEXISTS", key, field)

    async def hget(self, key: String, field: String) -> bytes:
        """Returns the value associated with field in the hash stored at key."""

        return await self._round_trip(b"HGET", key, field)

    async def hgetall(self, key: String) -> dict[bytes, bytes]:
        """
        :return: {field: value}

        Returns all fields and values of the hash stored at key. In the
        returned value, every field name is followed by its value, so the
        length of the reply is twice the size of the hash."""

        return await self._round_trip(b"HGETALL", key)

    async def hincrby(self, key: String, field: String, increment: int) -> int:
        """
        :return: value after increment
        :raise: RedisError

        Increments the number stored at field in the hash stored at key by
        increment. If key does not exist, a new key holding a hash is created.
        If field does not exist the value is set to 0 before the operation is
        performed.

        The range of values supported by HINCRBY is limited to 64 bit signed
        integers."""

        return await self._round_trip(b"HINCRBY", key, field, increment)

    async def hkeys(self, key: String) -> list[bytes]:
        """Returns all field names in the hash stored at key."""

        return await self._round_trip(b"HKEYS", key)

    async def hlen(self, key: String) -> int:
        """Returns the number of fields contained in the hash stored at key."""

        return await self._round_trip(b"HLEN", key)

    async def hmget(self, key: String, *fields: String) -> dict[bytes, bytes]:
        """
        :return: {field: value}

        Returns the values associated with the specified fields in the hash
        stored at key.

        For every field that does not exist in the hash, a nil value is
        returned. Because non-existing keys are treated as empty hashes,
        running HMGET against a non-existing key will return a list of nil
        values."""

        return await self._round_trip(b"HMGET", key, *fields)

    async def hset(self, key: String, *field_value: tuple[str, str]) -> int:
        """
        :return: number of successful set
        :raise: RedisError

        Sets field in the hash stored at key to value. If key does not
        exist, a new key holding a hash is created. If field already exists in
        the hash, it is overwritten."""

        def _args():
            yield key

            for field, value in field_value:
                yield field
                yield value

        return await self._round_trip(b"HSET", *_args())

    async def hrandfield(
        self, key: String, count: int = None, withvalues: bool = False
    ) -> bytes:
        """When called with just the key argument, return a random field from
        the hash value stored at key.

        If the provided count argument is positive, return an array of
        distinct fields. The array's length is either count or the hash's
        number of fields (HLEN), whichever is lower.

        If called with a negative count, the behavior changes and the command
        is allowed to return the same field multiple times. In this case, the
        number of returned fields is the absolute value of the specified
        count.

        The optional WITHVALUES modifier changes the reply so it includes the
        respective values of the randomly selected hash fields."""

        def _args():
            yield key

            if count is not None:
                yield count

            if withvalues:
                yield "WITHVALUES"

        return await self._round_trip(b"HRANDFIELD", *_args())

    async def hscan(
        self, key: String, cursor: int, pattern: String = None, count: int = None
    ) -> tuple[bytes, list[bytes]]:
        """
        :return: (cursor, [field])
        """

        def _args():
            yield key
            yield cursor

            if pattern:
                yield "MATCH"
                yield pattern
            if count is not None:
                yield "COUNT"
                yield count

        return await self._round_trip(b"HSCAN", *_args())

    async def hsetnx(self, key: String, field: String, value: String) -> int:
        """
        :return: either 0 or 1

        Sets field in the hash stored at key to value, only if field does
        not yet exist. If key does not exist, a new key holding a hash is
        created. If field already exists, this operation has no effect."""

        return await self._round_trip(b"HSETNX", key, field, value)

    async def hvals(self, key: String) -> list[bytes]:
        """Returns all values in the hash stored at key."""

        return await self._round_trip(b"HVALS", key)
