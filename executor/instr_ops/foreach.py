from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from executor.matching_ctx.buckets.f_bucket import F_Bucket
from schema import Instruction, InstructionType
from utils.dyn_graph import DynGraph


@OperatorFactory.register(InstructionType.Foreach)
class ForeachOperator(InstrOperator):
    """Foreach 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""

        C_bucket = self.ctx.resolve_C_pool(instr.single_op)
        F_bucket = F_Bucket.from_C_bucket(C_bucket)
        self.ctx.update_F_pool(instr.target_var, F_bucket)
