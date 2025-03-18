from typing import override
from warnings import deprecated

from executor.instr_ops.abc import InstrOperator
from schema import Instruction
from utils.dyn_graph import DynGraph

DEPRECATED_REASON = "TCache 暂时不会出现, 后续考虑如何进行 `三角形优化`"


@deprecated(DEPRECATED_REASON)
class TCacheOperator(InstrOperator):
    """TCache 指令算子"""

    @deprecated(DEPRECATED_REASON)
    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""
