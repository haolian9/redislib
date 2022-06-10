import logging

import IPython
import trio

from redislib import Redis


async def main():
    redis = Redis.from_addr("127.0.0.1", 6379, pool_size=3)

    async with redis:
        IPython.embed(header='available vars: redis')


if __name__ == "__main__":
    logging.basicConfig(
        level="DEBUG",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
        format="{asctime} {message}",
    )
    trio.run(main)
