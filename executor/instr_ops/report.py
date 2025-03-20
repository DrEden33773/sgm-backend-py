from typing import override

from executor.instr_ops.abc import InstrOperator
from schema import Instruction
from utils import dbg
from utils.dyn_graph import DynGraph


class ReportOperator(InstrOperator):
    """Report 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        dbg.pprint_instr(instr)

        f_pool = self.ctx.F_pool

        # 更新结果
        for f_bucket in f_pool.values():
            curr_group = f_bucket.all_matched
            result.append(curr_group)
