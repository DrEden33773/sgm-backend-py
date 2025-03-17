from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType
from utils.dyn_graph import DynGraph


@OperatorFactory.register(InstructionType.Report)
class ReportOperator(InstrOperator):
    """Report 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""

        f_pool = self.ctx.f_pool

        # 更新结果
        for f_bucket in f_pool.values():
            curr_group = f_bucket.all_matched
            result.append(curr_group)
