from abc import abstractmethod
from functools import lru_cache

from executor.matching_ctx import MatchingCtx
from schema import Instruction
from schema.basic import STR_TUPLE_SPLITTER, VarPrefix
from storage.abc import StorageAdapter
from utils.dyn_graph import DynGraph


class InstrOperator:
    """指令算子 (抽象基类)"""

    def __init__(self, storage_adapter: StorageAdapter, ctx: MatchingCtx) -> None:
        self.storage_adapter = storage_adapter
        self.ctx = ctx

    @lru_cache
    def resolve_var(self, target_var: str):
        """解析变量 -> (变量类型, 变量名)"""
        var_type, var_name = target_var.split(STR_TUPLE_SPLITTER)
        return VarPrefix(var_type), var_name

    @abstractmethod
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""
