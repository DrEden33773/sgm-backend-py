from abc import abstractmethod

from executor.matching_ctx import MatchingCtx
from schema import DisplayedInstr
from storage.abc import StorageAdapter


class InstrOperator:
    """指令算子 (抽象基类)"""

    def __init__(self, storage_adapter: StorageAdapter, ctx: MatchingCtx) -> None:
        self.storage_adapter = storage_adapter
        self.ctx = ctx

    @abstractmethod
    async def execute(self, instr: DisplayedInstr):
        """执行指令"""
