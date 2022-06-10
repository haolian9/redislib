from typing import Union

Cmd = bytes
Arg = Union[str, int, float, bytes]
String = Union[str, bytes]


class RoundTrip:
    async def _round_trip(self, cmd: Cmd, *args: Arg):
        raise NotImplementedError
