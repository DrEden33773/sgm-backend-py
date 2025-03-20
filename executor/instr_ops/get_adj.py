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
        f_bucket = self.ctx.resolve_f_bucket(instr.single_op)
        A_bucket = A_Bucket.from_f_bucket(curr_pat_vid, f_bucket)

        # 按照 `下一个点的模式`, `分组` 加载边
        for pattern_e in pattern_es:
            label, attr = pattern_e.label, pattern_e.attr

            data_es = (
                self.storage_adapter.load_e(label)
                if not attr
                else self.storage_adapter.load_e_with_attr(label, attr)
            )

            # 注意: next_pat_vid 不要根据 pattern_e 的终点加载
            next_pat_vid = (
                pattern_e.dst_vid
                if curr_pat_vid == pattern_e.src_vid
                else pattern_e.src_vid
            )

            A_bucket.with_new_edges(data_es, next_pat_vid)

        # A_bucket 过滤: 选出 `可连接的边` 和 `被连接的图`
        connected_data_vids = A_bucket.select_connective_edges_and_graphs(
            self.ctx.pattern_vs, self.storage_adapter
        )

        # 更新容器 (以及 `已被连接的点集`)
        self.ctx.update_A_pool(instr.target_var, A_bucket)
        self.ctx.update_connected_data_vids(connected_data_vids)
