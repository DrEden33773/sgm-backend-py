from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.matching_ctx.buckets import f_Bucket
from schema import Instruction
from utils import dbg
from utils.dyn_graph import DynGraph


class ForeachOperator(InstrOperator):
    """Foreach 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        dbg.pprint_instr(instr)

        # 先初始化 ctx 中 f_pool 对应位置
        self.ctx.init_f_pool(instr.target_var)

        C_bucket = self.ctx.resolve_C_pool(instr.single_op or "")
        if not C_bucket:
            return

        f_bucket = f_Bucket.from_C_bucket(C_bucket)
        self.ctx.update_f_pool(instr.target_var, f_bucket)
