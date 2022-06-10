from typing import Union

from ..typing import RoundTrip, String


class ZsetsMixin(RoundTrip):
    """https://redis.io/commands#sorted-set"""

    async def zadd(
        self,
        key: String,
        *score_member: tuple[float, str],
        nx: bool = False,
        xx: bool = False,
        gt: bool = False,
        lt: bool = False,
        ch: bool = False,
        incr: bool = False,
    ):
        """Adds all the specified members with the specified scores to the
        sorted set stored at key. It is possible to specify multiple score
        / member pairs. If a specified member is already a member of the
        sorted set, the score is updated and the element reinserted at the
        right position to ensure the correct ordering.

        If key does not exist, a new sorted set with the specified members as
        sole members is created, like if the sorted set was empty. If the key
        exists but does not hold a sorted set, an error is returned.

        The score values should be the string representation of a double
        precision floating point number. +inf and -inf values are valid values
        as well.

        ZADD options

        ZADD supports a list of options, specified after the name of the key and before the first score argument. Options are:

        XX: Only update elements that already exist. Don't add new elements.
        NX: Only add new elements. Don't update already existing elements.
        LT: Only update existing elements if the new score is less than the
            current score. This flag doesn't prevent adding new elements.
        GT: Only update existing elements if the new score is greater than the
            current score. This flag doesn't prevent adding new elements.
        CH: Modify the return value from the number of new elements added, to
            the total number of elements changed (CH is an abbreviation of
            changed). Changed elements are new elements added and elements
            already existing for which the score was updated. So elements
            specified in the command line having the same score as they had in the
            past are not counted. Note: normally the return value of ZADD only
            counts the number of new elements added.
        INCR: When this option is specified ZADD acts like ZINCRBY. Only one
            score-element pair can be specified in this mode.

        Note: The GT, LT and NX options are mutually exclusive.

        Range of integer scores that can be expressed precisely

        Redis sorted sets use a double 64-bit floating point number to
        represent the score. In all the architectures we support, this is
        represented as an IEEE 754 floating point number, that is able to
        represent precisely integer numbers between -(2^53) and +(2^53)
        included. In more practical terms, all the integers between
        -9007199254740992 and 9007199254740992 are perfectly representable.
        Larger integers, or fractions, are internally represented in
        exponential form, so it is possible that you get only an approximation
        of the decimal number, or of the very big integer, that you set as
        score.

        Sorted sets 101

        Sorted sets are sorted by their score in an ascending way. The same
        element only exists a single time, no repeated elements are permitted.
        The score can be modified both by ZADD that will update the element
        score, and as a side effect, its position on the sorted set, and by
        ZINCRBY that can be used in order to update the score relatively to
        its previous value.

        The current score of an element can be retrieved using the ZSCORE
        command, that can also be used to verify if an element already exists
        or not.

        For an introduction to sorted sets, see the data types page on sorted sets.

        Elements with the same score

        While the same element can't be repeated in a sorted set since every
        element is unique, it is possible to add multiple different elements
        having the same score. When multiple elements have the same score,
        they are ordered lexicographically (they are still ordered by score as
        a first key, however, locally, all the elements with the same
        score are relatively ordered lexicographically).

        The lexicographic ordering used is binary, it compares strings as array of bytes.

        If the user inserts all the elements in a sorted set with the same
        score (for example 0), all the elements of the sorted set are sorted
        lexicographically, and range queries on elements are possible using
        the command ZRANGEBYLEX (Note: it is also possible to query sorted
                sets by range of scores using ZRANGEBYSCORE).
        """

        def _args():
            yield key

            if nx:
                yield "NX"
            if xx:
                yield "XX"
            if gt:
                yield "GT"
            if lt:
                yield "LT"
            if ch:
                yield "CH"
            if incr:
                yield "INCR"

            for score, member in score_member:
                yield score
                yield member

        return await self._round_trip(b"ZADD", *_args())

    async def zcard(self, key: String):
        """Returns the sorted set cardinality (number of elements) of the
        sorted set stored at key."""

        return await self._round_trip(b"ZCARD", key)

    async def zrange(
        self,
        key: String,
        min: Union[int, float],
        max: Union[int, float],
        byscore: bool = False,
        bylex: bool = False,
        rev: bool = False,
        offset: int = None,
        count: int = None,
        withscores: bool = False,
    ):
        """Returns the specified range of elements in the sorted set stored at <key>.

        ZRANGE can perform different types of range queries: by index (rank),
        by the score, or by lexicographical order.

        Starting with Redis 6.2.0, this command can replace the following
        commands: ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANGEBYLEX and
        ZREVRANGEBYLEX.

        Common behavior and options

        The order of elements is from the lowest to the highest score.
        Elements with the same score are ordered lexicographically.

        The optional REV argument reverses the ordering, so elements are
        ordered from highest to lowest score, and score ties are resolved by
        reverse lexicographical ordering.

        The optional LIMIT argument can be used to obtain a sub-range from the
        matching elements (similar to SELECT LIMIT offset, count in SQL).
        A negative <count> returns all elements from the <offset>. Keep in
        mind that if <offset> is large, the sorted set needs to be traversed
        for <offset> elements before getting to the elements to return, which
        can add up to O(N) time complexity.

        The optional WITHSCORES argument supplements the command's reply with
        the scores of elements returned. The returned list contains
        value1,score1,...,valueN,scoreN instead of value1,...,valueN. Client
        libraries are free to return a more appropriate data type (suggestion:
                an array with (value, score) arrays/tuples).

        Index ranges

        By default, the command performs an index range query. The <min> and
        <max> arguments represent zero-based indexes, where 0 is the first
        element, 1 is the next element, and so on. These arguments specify an
        inclusive range, so for example, ZRANGE myzset 0 1 will return both
        the first and the second element of the sorted set.

        The indexes can also be negative numbers indicating offsets from the
        end of the sorted set, with -1 being the last element of the sorted
        set, -2 the penultimate element, and so on.

        Out of range indexes do not produce an error.

        If <min> is greater than either the end index of the sorted set or
        <max>, an empty list is returned.

        If <max> is greater than the end index of the sorted set, Redis will
        use the last element of the sorted set.

        Score ranges

        When the BYSCORE option is provided, the command behaves like
        ZRANGEBYSCORE and returns the range of elements from the sorted set
        having scores equal or between <min> and <max>.

        <min> and <max> can be -inf and +inf, denoting the negative and
        positive infinities, respectively. This means that you are not
        required to know the highest or lowest score in the sorted set to get
        all elements from or up to a certain score.

        By default, the score intervals specified by <min> and <max> are
        closed (inclusive). It is possible to specify an open interval
        (exclusive) by prefixing the score with the character (.

        For example:

            ZRANGE zset (1 5 BYSCORE
        Will return all elements with 1 < score <= 5 while:

            ZRANGE zset (5 (10 BYSCORE
        Will return all the elements with 5 < score < 10 (5 and 10 excluded).

        Lexicographical ranges

        When the BYLEX option is used, the command behaves like ZRANGEBYLEX
        and returns the range of elements from the sorted set between the
        <min> and <max> lexicographical closed range intervals.

        Note that lexicographical ordering relies on all elements having the
        same score. The reply is unspecified when the elements have different
        scores.

        Valid <min> and <max> must start with ( or [, in order to specify
            whether the range interval is exclusive or inclusive,
            respectively.

        The special values of + or - <min> and <max> mean positive and
        negative infinite strings, respectively, so for instance the command
        ZRANGEBYLEX myzset - + is guaranteed to return all the elements in the
        sorted set, providing that all the elements have the same score.

        Lexicographical comparison of strings

        Strings are compared as a binary array of bytes. Because of how the
        ASCII character set is specified, this means that usually this also
        have the effect of comparing normal ASCII characters in an obvious
        dictionary way. However, this is not true if non-plain ASCII strings
        are used (for example, utf8 strings).

        However, the user can apply a transformation to the encoded string so
        that the first part of the element inserted in the sorted set will
        compare as the user requires for the specific application. For
        example, if I want to add strings that will be compared in
        a case-insensitive way, but I still want to retrieve the real case
        when querying, I can add strings in the following way:

            ZADD autocomplete 0 foo:Foo 0 bar:BAR 0 zap:zap

        Because of the first normalized part in every element (before the
            colon character), we are forcing a given comparison. However,
        after the range is queried using ZRANGE ... BYLEX, the application can
        display to the user the second part of the string, after the colon.

        The binary nature of the comparison allows to use sorted sets as
        a general purpose index, for example, the first part of the element
        can be a 64-bit big-endian number. Since big-endian numbers have the
        most significant bytes in the initial positions, the binary comparison
        will match the numerical comparison of the numbers. This can be used
        in order to implement range queries on 64-bit values. As in the
        example below, after the first 8 bytes, we can store the value of the
        element we are indexing."""

        def _args():
            yield key
            yield min
            yield max

            if byscore:
                yield "BYSCORE"
            if bylex:
                yield "BYLEX"
            if rev:
                yield "REV"
            if offset is not None and count is not None:
                yield "LIMIT"
                yield offset
                yield count
            if withscores:
                yield "WITHSCORES"

        return await self._round_trip(b"ZRANGE", *_args())

    async def zrank(self, key: String, member: String):
        """Returns the rank of member in the sorted set stored at key, with
        the scores ordered from low to high. The rank (or index) is 0-based,
        which means that the member with the lowest score has rank 0.

        Use ZREVRANK to get the rank of an element with the scores ordered
        from high to low."""

        return await self._round_trip(b"ZRANK", key, member)

    async def zrem(self, key: String, *members: String):
        """Removes the specified members from the sorted set stored at key.
        Non existing members are ignored."""

        return await self._round_trip(b"ZREM", key, *members)

    async def zremrangebyrank(self, key: String, start: int, stop: int):
        """Removes all elements in the sorted set stored at key with rank
        between start and stop. Both start and stop are 0 -based indexes with
        0 being the element with the lowest score. These indexes can be
        negative numbers, where they indicate offsets starting at the element
        with the highest score. For example: -1 is the element with the
        highest score, -2 the element with the second highest score and so
        forth."""

        return await self._round_trip(b"ZREMRANGEBYRANK", key, start, stop)

    async def zremrangebyscore(self, key: String, min: float, max: float):
        """Removes all elements in the sorted set stored at key with a score between min and max (inclusive)."""
        return await self._round_trip(b"ZREMRANGEBYSCORE", key, min, max)

    async def zscan(
        self, key: String, cursor: int, pattern: String = None, count: int = None
    ):
        def _args():
            yield key
            yield cursor

            if pattern:
                yield "MATCH"
                yield pattern
            if count is not None:
                yield "COUNT"
                yield count

        return await self._round_trip(b"ZSCAN", *_args())

    async def zscore(self, key: String, member: String):
        """Returns the score of member in the sorted set at key.

        If member does not exist in the sorted set, or key does not exist, nil
        is returned."""

        return await self._round_trip(b"ZSCORE", key, member)

    async def zpopmax(self, key: String, count: int = None):
        """Removes and returns up to count members with the highest scores in
        the sorted set stored at key.

        When left unspecified, the default value for count is 1. Specifying
        a count value that is higher than the sorted set's cardinality will
        not produce an error. When returning multiple elements, the one with
        the highest score will be the first, followed by the elements with
        lower scores."""

        def _args():
            yield key
            if count is not None:
                yield count

        return await self._round_trip(b"ZPOPMAX", *_args())

    async def zpopmin(self, key: String, count: int = None):
        """Removes and returns up to count members with the lowest scores in
        the sorted set stored at key.

        When left unspecified, the default value for count is 1. Specifying
        a count value that is higher than the sorted set's cardinality will
        not produce an error. When returning multiple elements, the one with
        the lowest score will be the first, followed by the elements with
        greater scores."""

        def _args():
            yield key
            if count is not None:
                yield count

        return await self._round_trip(b"ZPOPMIN", *_args())
