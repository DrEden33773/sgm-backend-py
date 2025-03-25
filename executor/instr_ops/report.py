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

        if self.ctx.empty_matched_set_appeared:
            # 如果出现了空的匹配集, 那么就不应该进行 `Report` 操作
            # 但是, 出于一致性原则, 还是把 result 给初始化好
            for _ in range(len(f_pool)):
                result.append([])
            return

        def could_match_partial_pattern(graph: DynGraph):
            graph_v_pat_cnt = {
                v_pat: list(graph.v_2_pattern.values()).count(v_pat)
                for v_pat in set(graph.v_2_pattern.values())
            }
            graph_e_pat_cnt = {
                e_pat: list(graph.e_2_pattern.values()).count(e_pat)
                for e_pat in set(graph.e_2_pattern.values())
            }
            plan_v_pat_cnt = {v_pat: 1 for v_pat in self.ctx.plan_data.pattern_vs}
            plan_e_pat_cnt = {e_pat: 1 for e_pat in self.ctx.plan_data.pattern_es}

            for v_pat, cnt in graph_v_pat_cnt.items():
                if v_pat not in plan_v_pat_cnt or cnt > plan_v_pat_cnt[v_pat]:
                    return False
            for e_pat, cnt in graph_e_pat_cnt.items():
                if e_pat not in plan_e_pat_cnt or cnt > plan_e_pat_cnt[e_pat]:
                    return False
            return True

        # 更新结果
        #
        # 由于 `Intersect(T, A)` 的 `union_then_intersect_on_connective_v` 策略
        # 可能导致匹配出的图 `规模更大` (相较于模式图)
        #
        # 所以要根据 `点数` 和 `边数` 进行最终过滤
        #
        # 过滤的策略不能是 `==`, 因为如果 `模式图` 是 `森林`, 那么
        # `有效的匹配` 将会以 `多个连通集 (森林子图)` 的形式存在, 不能把它们过滤掉
        #
        # 正确的过滤策略应该是 `有效匹配规模` <= `模式图规模`
        for f_bucket in f_pool.values():
            curr_group = f_bucket.all_matched
            filtered = [
                matched
                for matched in curr_group
                if could_match_partial_pattern(matched)
            ]
            result.append(filtered)
