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
        #
        # 由于 `Intersect(T, A)` 的 `union_then_intersect_on_connective_v` 策略
        # 可能导致匹配出的图 `规模更大` (相较于模式图)
        #
        # 所以要根据 `点数` 和 `边数` 进行最终过滤
        for f_bucket in f_pool.values():
            curr_group = f_bucket.all_matched
            filtered = [
                matched
                for matched in curr_group
                if matched.get_v_count() == len(self.ctx.pattern_vs)
                and matched.get_e_count() == len(self.ctx.pattern_es)
            ]
            result.append(filtered)
