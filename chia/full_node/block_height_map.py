import aiosqlite
import logging
from typing import Dict, List, Optional, Tuple
from chia.util.ints import uint32
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.sub_epoch_summary import SubEpochSummary

log = logging.getLogger(__name__)


class BlockHeightMap:
    db: aiosqlite.Connection

    # the below dictionaries are loaded from the database lazily, from the peak
    # and back in time, on demand. The following members are to support resuming
    # loading of blocks where we last left off.

    # the block hash of the next block to load, i.e. the block prior to the
    # lowest height block loaded so far
    __prev_hash: Optional[bytes32] = None

    # the lowest height block currently loaded into the dictionaries. The next
    # block we load is expected to be exactly one less than this
    __lowest_height: uint32 = uint32(0)

    # Defines the path from genesis to the peak, no orphan blocks
    __height_to_hash: Dict[uint32, bytes32]
    # All sub-epoch summaries that have been included in the blockchain from the beginning until and including the peak
    # (height_included, SubEpochSummary). Note: ONLY for the blocks in the path to the peak
    __sub_epoch_summaries: Dict[uint32, SubEpochSummary]

    @classmethod
    async def create(cls, db: aiosqlite.Connection) -> "BlockHeightMap":
        self = BlockHeightMap()
        self.db = db

        self.__height_to_hash = {}
        self.__sub_epoch_summaries = {}

        res = await self.db.execute(
            "SELECT header_hash,prev_hash,height,sub_epoch_summary from block_records WHERE is_peak=1"
        )
        row = await res.fetchone()
        await res.close()

        if row is None:
            return self

        peak: bytes32 = bytes.fromhex(row[0])
        self.__prev_hash = bytes.fromhex(row[1])
        self.__lowest_height = row[2]
        self.__height_to_hash[self.__lowest_height] = peak
        if row[3] is not None:
            self.__sub_epoch_summaries[self.__lowest_height] = SubEpochSummary.from_bytes(row[3])

        # prepopulate the height -> hash mapping
        await self._load_blocks_to(uint32(0))

        return self

    def update_height(self, height: uint32, header_hash: bytes32, ses: Optional[SubEpochSummary]):
        assert height >= self.__lowest_height
        self.__height_to_hash[height] = header_hash
        if ses is not None:
            self.__sub_epoch_summaries[height] = ses

    # loads blocks from where we last left off down to the specified height
    async def _load_blocks_to(self, height: uint32):
        assert self.__lowest_height > height

        cursor = await self.db.execute(
            "SELECT header_hash,prev_hash,height,sub_epoch_summary from block_records WHERE height>=? AND height <?",
            (height, self.__lowest_height),
        )

        assert self.__prev_hash is not None
        rows = await cursor.fetchall()
        await cursor.close()

        # maps block-hash -> (height, prev-hash, sub-epoch-summary)
        ordered: Dict[bytes32, Tuple[uint32, bytes32, Optional[bytes]]] = {}
        for r in rows:
            ordered[bytes.fromhex(r[0])] = (r[2], bytes.fromhex(r[1]), r[3])

        while self.__lowest_height > height:
            entry = ordered[self.__prev_hash]
            assert entry[0] == self.__lowest_height - 1
            self.__height_to_hash[entry[0]] = self.__prev_hash
            if entry[2] is not None:
                self.__sub_epoch_summaries[entry[0]] = SubEpochSummary.from_bytes(entry[2])
            self.__prev_hash = entry[1]
            self.__lowest_height = entry[0]

    def get_hash(self, height: uint32) -> bytes32:
        return self.__height_to_hash[height]

    def contains_height(self, height: uint32) -> bool:
        if height < self.__lowest_height:
            return True
        return height in self.__height_to_hash

    def rollback(self, fork_height: int):
        # fork height may be -1, in which case all blocks are different and we
        # should clear all sub epoch summaries
        heights_to_delete = []
        for ses_included_height in self.__sub_epoch_summaries.keys():
            if ses_included_height > fork_height:
                heights_to_delete.append(ses_included_height)
        for height in heights_to_delete:
            log.info(f"delete ses at height {height}")
            del self.__sub_epoch_summaries[height]

    def get_ses(self, height: uint32) -> SubEpochSummary:
        return self.__sub_epoch_summaries[height]

    # TODO: This function is not sustainable
    def get_ses_heights(self) -> List[uint32]:
        return sorted(self.__sub_epoch_summaries.keys())
