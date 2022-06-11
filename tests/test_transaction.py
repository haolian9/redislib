import pytest
from redislib import Redis


async def test_open_tx(redis: Redis):
    async with redis.open_transaction() as tx:
        assert b"OK" == await tx.multi()

        await tx.set("tx:2:1", '2:1', nx=True)
        await tx.set("tx:2:2", '2:2', nx=True)
        assert [b"OK"] * 2 == await tx.exec()


async def test_tx_without_exec(redis: Redis):
    with pytest.raises(RuntimeError) as err:
        async with redis.open_transaction() as tx:
            assert b"OK" == await tx.multi()

            await tx.set("tx:2:1", '2:1', nx=True)
            await tx.set("tx:2:2", '2:2', nx=True)

    assert err.value.args[0] == "unmatched transaction acquiring and releasing"
