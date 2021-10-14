import aiosqlite
import pytest
import struct
from chia.full_node.block_height_map import BlockHeightMap
from chia.types.blockchain_format.sub_epoch_summary import SubEpochSummary

from tests.util.db_connection import DBConnection
from chia.types.blockchain_format.sized_bytes import bytes32
from typing import Optional
from chia.util.ints import uint8


def gen_block_hash(height: int) -> bytes32:
    return struct.pack(">I", height + 1) * (32 // 4)


def gen_ses(height: int) -> SubEpochSummary:
    prev_ses = gen_block_hash(height + 0xFA0000)
    reward_chain_hash = gen_block_hash(height + 0xFC0000)
    return SubEpochSummary(prev_ses, reward_chain_hash, uint8(0), None, None)


async def new_block(
    db: aiosqlite.Connection,
    block_hash: bytes32,
    parent: bytes32,
    height: int,
    is_peak: bool,
    ses: Optional[SubEpochSummary],
):
    cursor = await db.execute(
        "INSERT INTO block_records VALUES(?, ?, ?, ?, ?)",
        (
            block_hash.hex(),
            parent.hex(),
            height,
            # sub epoch summary
            None if ses is None else bytes(ses),
            is_peak,
        ),
    )
    await cursor.close()


async def setup_db(db: aiosqlite.Connection):
    await db.execute(
        "CREATE TABLE IF NOT EXISTS block_records("
        "header_hash text PRIMARY KEY,"
        "prev_hash text,"
        "height bigint,"
        "sub_epoch_summary blob,"
        "is_peak tinyint)"
    )
    await db.execute("CREATE INDEX IF NOT EXISTS hh on block_records(header_hash)")
    await db.execute("CREATE INDEX IF NOT EXISTS peak on block_records(is_peak)")


# if chain_id != 0, the last block in the chain won't be considered the peak,
# and the chain_id will be mixed in to the hashes, to form a separate chain at
# the same heights as the main chain
async def setup_chain(db: aiosqlite.Connection, length: int, *, chain_id: int = 0, ses_every: Optional[int] = None):
    height = 0
    peak_hash = gen_block_hash(height + chain_id * 65536)
    parent_hash = bytes32([0] * 32)
    while height < length:
        ses = None
        if ses_every is not None and height % ses_every == 0:
            ses = gen_ses(height)

        await new_block(db, peak_hash, parent_hash, height, False, ses)
        height += 1
        parent_hash = peak_hash
        peak_hash = gen_block_hash(height + chain_id * 65536)

    # we only set is_peak=1 for chain_id 0
    await new_block(db, peak_hash, parent_hash, height, chain_id == 0, None)


class TestBlockHeightMap:
    @pytest.mark.asyncio
    async def test_height_to_hash(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            assert not height_map.contains_height(11)
            for height in reversed(range(10)):
                assert height_map.contains_height(height)

            for height in reversed(range(10)):
                assert height_map.get_hash(height) == gen_block_hash(height)

    @pytest.mark.asyncio
    async def test_height_to_hash_long_chain(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10000)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            for height in reversed(range(1000)):
                assert height_map.contains_height(height)

            for height in reversed(range(10000)):
                assert height_map.get_hash(height) == gen_block_hash(height)

    @pytest.mark.asyncio
    async def test_height_to_hash_with_orphans(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10)

            # set up two separate chains, but without the peak
            await setup_chain(db_wrapper.db, 10, chain_id=1)
            await setup_chain(db_wrapper.db, 10, chain_id=2)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            for height in range(10):
                assert height_map.get_hash(height) == gen_block_hash(height)

    @pytest.mark.asyncio
    async def test_height_to_hash_update(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10)

            # orphan blocks
            await setup_chain(db_wrapper.db, 10, chain_id=1)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            for height in range(10):
                assert height_map.get_hash(height) == gen_block_hash(height)

            height_map.update_height(10, gen_block_hash(100), None)

            for height in range(9):
                assert height_map.get_hash(height) == gen_block_hash(height)

            assert height_map.get_hash(10) == gen_block_hash(100)

    @pytest.mark.asyncio
    async def test_update_ses(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10)

            # orphan blocks
            await setup_chain(db_wrapper.db, 10, chain_id=1)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            with pytest.raises(KeyError) as _:
                height_map.get_ses(10)

            height_map.update_height(10, gen_block_hash(10), gen_ses(10))

            assert height_map.get_ses(10) == gen_ses(10)
            assert height_map.get_hash(10) == gen_block_hash(10)

    @pytest.mark.asyncio
    async def test_height_to_ses(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10, ses_every=2)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            assert height_map.get_ses(0) == gen_ses(0)
            assert height_map.get_ses(2) == gen_ses(2)
            assert height_map.get_ses(4) == gen_ses(4)
            assert height_map.get_ses(6) == gen_ses(6)
            assert height_map.get_ses(8) == gen_ses(8)

            with pytest.raises(KeyError) as _:
                height_map.get_ses(1)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(3)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(5)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(7)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(9)

    @pytest.mark.asyncio
    async def test_rollback(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10, ses_every=2)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            assert height_map.get_ses(0) == gen_ses(0)
            assert height_map.get_ses(2) == gen_ses(2)
            assert height_map.get_ses(4) == gen_ses(4)
            assert height_map.get_ses(6) == gen_ses(6)
            assert height_map.get_ses(8) == gen_ses(8)

            height_map.rollback(5)

            assert height_map.get_ses(0) == gen_ses(0)
            assert height_map.get_ses(2) == gen_ses(2)
            assert height_map.get_ses(4) == gen_ses(4)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(6)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(8)

    @pytest.mark.asyncio
    async def test_rollback2(self):

        async with DBConnection() as db_wrapper:

            await setup_db(db_wrapper.db)
            await setup_chain(db_wrapper.db, 10, ses_every=2)

            height_map = await BlockHeightMap.create(db_wrapper.db)

            assert height_map.get_ses(0) == gen_ses(0)
            assert height_map.get_ses(2) == gen_ses(2)
            assert height_map.get_ses(4) == gen_ses(4)
            assert height_map.get_ses(6) == gen_ses(6)
            assert height_map.get_ses(8) == gen_ses(8)

            height_map.rollback(6)

            assert height_map.get_ses(0) == gen_ses(0)
            assert height_map.get_ses(2) == gen_ses(2)
            assert height_map.get_ses(4) == gen_ses(4)
            assert height_map.get_ses(6) == gen_ses(6)
            with pytest.raises(KeyError) as _:
                height_map.get_ses(8)
