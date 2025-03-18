from typing import Type

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.foreach import ForeachOperator
from executor.instr_ops.get_adj import GetAdjOperator
from executor.instr_ops.init import InitOperator
from executor.instr_ops.intersect import IntersectOperator
from executor.instr_ops.report import ReportOperator
from executor.matching_ctx import MatchingCtx
from schema import Instruction, InstructionType
from storage.abc import StorageAdapter


class OperatorFactory:
    """指令算子工厂"""

    _operators: dict[InstructionType, Type[InstrOperator]] = {
        InstructionType.Init: InitOperator,
        InstructionType.Foreach: ForeachOperator,
        InstructionType.GetAdj: GetAdjOperator,
        InstructionType.Intersect: IntersectOperator,
        InstructionType.Report: ReportOperator,
    }

    @classmethod
    def create(
        cls,
        instr: Instruction,
        storage_adapter: StorageAdapter,
        ctx: MatchingCtx,
    ) -> InstrOperator:
        """创建指令算子"""

        operator_cls = cls._operators.get(instr.type)
        if operator_cls is None:
            raise ValueError(f"Unknown instruction type: {instr.type}")
        return operator_cls(storage_adapter, ctx)
