from typing import Literal

from ..typing import RoundTrip, String


class ListsMixin(RoundTrip):
    """
    left -> right: first -> last
    """

    async def llen(self, key: String):
        """Returns the length of the list stored at key.
        If key does not exist, it is interpreted as an empty list and 0 is returned.
        An error is returned when the value stored at key is not a list.
        """
        return await self._round_trip(b"LLEN", key)

    async def lpush(self, key: String, *elements: String):
        """Insert all the specified values at the head of the list stored at key.
        If key does not exist, it is created as empty list before performing
        the push operations. When key holds a value that is not a list, an
        error is returned.

        It is possible to push multiple elements using a single command call
        just specifying multiple arguments at the end of the command. Elements
        are inserted one after the other to the head of the list, from the
        leftmost element to the rightmost element. So for instance the command
        LPUSH mylist a b c will result into a list containing c as first
        element, b as second element and a as third element."""
        return await self._round_trip(b"LPUSH", key, *elements)

    async def lpop(self, key: String, n: int = 1):
        """
        Removes and returns the first elements of the list stored at key.
        By default, the command pops a single element from the beginning of the list. When provided with the optional count argument, the reply will consist of up to count elements, depending on the list's length.

        Return value
        When called without the count argument:
            Bulk string reply: the value of the first element, or nil when key does not exist.
        When called with the count argument:
            Array reply: list of popped elements, or nil when key does not exist.
        """
        return await self._round_trip(b"LPOP", key, n)

    async def rpush(self, key: String, *elements: String):
        """Insert all the specified values at the tail of the list stored at
        key. If key does not exist, it is created as empty list before
        performing the push operation. When key holds a value that is not
        a list, an error is returned.

        It is possible to push multiple elements using a single command call just
        specifying multiple arguments at the end of the command. Elements are inserted
        one after the other to the tail of the list, from the leftmost element to the
        rightmost element. So for instance the command RPUSH mylist a b c will result
        into a list containing a as first element, b as second element and c as third
        element."""
        return await self._round_trip(b"RPUSH", key, *elements)

    async def rpop(self, key: String, n: int = None):
        """
        Removes and returns the last elements of the list stored at key.
        By default, the command pops a single element from the end of the list. When provided with the optional count argument, the reply will consist of up to count elements, depending on the list's length.

        Return value
        When called without the count argument:
            Bulk string reply: the value of the last element, or nil when key does not exist.
        When called with the count argument:
            Array reply: list of popped elements, or nil when key does not exist.
        """

        def _args():
            yield key

            if n is not None:
                yield n

        return await self._round_trip(b"RPOP", *_args())

    async def lrange(self, key: str, start: int, stop: int):
        """Returns the specified elements of the list stored at key. The
        offsets start and stop are zero-based indexes, with 0 being the first
        element of the list (the head of the list), 1 being the next element
        and so on.

        These offsets can also be negative numbers indicating offsets starting
        at the end of the list. For example, -1 is the last element of the
        list, -2 the penultimate, and so on.

        Consistency with range functions in various programming languages

        Note that if you have a list of numbers from 0 to 100, LRANGE list
        0 10 will return 11 elements, that is, the rightmost item is included.
        This may or may not be consistent with behavior of range-related
        functions in your programming language of choice (think Ruby's
                Range.new, Array#slice or Python's range() function).

        Out-of-range indexes

        Out of range indexes will not produce an error. If start is larger
        than the end of the list, an empty list is returned. If stop is larger
        than the actual end of the list, Redis will treat it like the last
        element of the list."""

        return await self._round_trip(b"LRANGE", key, start, stop)

    async def ltrim(self, key: str, start: int, stop: int):
        """Trim an existing list so that it will contain only the specified
        range of elements specified. Both start and stop are zero-based
        indexes, where 0 is the first element of the list (the head), 1 the
        next element and so on.

        For example: `LTRIM foobar 0 2` will modify the list stored at foobar so
        that only the first three elements of the list will remain.

        start and end can also be negative numbers indicating offsets from the
        end of the list, where -1 is the last element of the list, -2 the
        penultimate element and so on.

        Out of range indexes will not produce an error: if start is larger
        than the end of the list, or start > end, the result will be an empty
        list (which causes key to be removed). If end is larger than the end
        of the list, Redis will treat it like the last element of the list.

        A common use of LTRIM is together with LPUSH / RPUSH. For example:
        * LPUSH mylist someelement
        * LTRIM mylist 0 99

        This pair of commands will push a new element on the list, while
        making sure that the list will not grow larger than 100 elements. This
        is very useful when using Redis to store logs for example. It is
        important to note that when used in this way LTRIM is an O(1)
        operation because in the average case just one element is removed from
        the tail of the list."""

        return await self._round_trip(b"LTRIM", key, start, stop)

    async def lmove(
        self,
        source: str,
        destination: str,
        source_left: Literal[True] = None,
        source_right: Literal[True] = None,
        destination_left: Literal[True] = None,
        destination_right: Literal[True] = None,
    ):
        """Atomically returns and removes the first/last element (head/tail
        depending on the wherefrom argument) of the list stored at
        source, and pushes the element at the first/last element (head/tail
        depending on the whereto argument) of the list stored at destination.

        For example: consider source holding the list a,b,c, and destination
        holding the list x,y,z. Executing LMOVE source destination RIGHT LEFT
        results in source holding a,b and destination holding c,x,y,z.

        If source does not exist, the value nil is returned and no operation
        is performed. If source and destination are the same, the operation is
        equivalent to removing the first/last element from the list and
        pushing it as first/last element of the list, so it can be considered
        as a list rotation command (or a no-op if wherefrom is the same as whereto).

        This command comes in place of the now deprecated RPOPLPUSH. Doing
        LMOVE RIGHT LEFT is equivalent."""

        no_source_ends = not (source_left or source_right)
        has_dest_ends = destination_left or destination_right
        if no_source_ends and has_dest_ends:
            raise ValueError(
                "as destination end is given, source end should be given also"
            )

        def _args():
            yield source
            yield destination

            if source_left:
                yield "LEFT"
            if source_right:
                yield "RIGHT"

            if destination_left:
                yield "LEFT"
            if destination_right:
                yield "RIGHT"

        # TODO@haoliang fixme
        return await self._round_trip(b"LMOVE", *_args())
