from abc import abstractmethod
from functools import lru_cache

from executor.matching_ctx import MatchingCtx
from executor.matching_ctx.type_aliases import DgVid, PgEid
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

    def does_data_v_satisfy_pattern(
        self,
        dg_vid: DgVid,
        pat_vid: PgEid,
    ) -> bool:
        pattern_v = self.ctx.get_pattern_v(pat_vid)
        data_v = self.storage_adapter.get_v(dg_vid)

        # 比较标签
        if pattern_v.label != data_v.label:
            return False

        # 比较属性
        if not pattern_v.attr:
            # 模式点未规定属性
            return True
        elif not data_v.attr:
            # 模式点规定了属性, 但是数据点没有属性
            return False
        else:
            operator = pattern_v.attr.op.to_operator()
            data_v_attr_value = data_v.attr
            pattern_v_attr_value = pattern_v.attr.value
            if type(data_v_attr_value) is not type(pattern_v_attr_value):
                return False
            return operator(data_v_attr_value, pattern_v_attr_value)

    @abstractmethod
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""
