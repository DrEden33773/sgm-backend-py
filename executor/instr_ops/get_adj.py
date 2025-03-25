from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.matching_ctx import A_Bucket
from schema import Instruction
from utils import dbg
from utils.dyn_graph import DynGraph


class GetAdjOperator(InstrOperator):
    """GetAdj 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        dbg.pprint_instr(instr)

        # 解析出, 当前的 `模式顶点`
        _, curr_pat_vid = self.resolve_var(instr.single_op)

        pattern_es = self.ctx.get_pattern_e_batch(instr.expand_eid_list)
        pattern_vs = self.ctx.pattern_vs
        f_bucket = self.ctx.resolve_f_bucket(instr.single_op)
        A_bucket = A_Bucket.from_f_bucket(curr_pat_vid, f_bucket)

        # 先初始化 ctx 中 A_pool 对应位置
        self.ctx.init_A_pool(instr.target_var)

        # 直接调用新的 `增量边载入` 逻辑
        connected_data_vids = A_bucket.incremental_load_new_edges(
            pattern_es, pattern_vs, self.storage_adapter
        )

        # 更新容器 (以及 `已被扩张的点集`)
        self.ctx.update_A_pool(instr.target_var, A_bucket)
        self.ctx.update_expanded_data_vids(connected_data_vids)
