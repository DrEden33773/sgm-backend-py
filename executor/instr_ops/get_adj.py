from typing import override

from config import DIRECTED_EDGE_SUPPORT, USE_INCREMENTAL_EDGES_LOADING
from executor.instr_ops.abc import InstrOperator
from executor.matching_ctx import A_Bucket
from executor.matching_ctx.buckets import does_data_v_satisfy_pattern
from schema import Instruction
from utils import dbg
from utils.dyn_graph import DynGraph


class GetAdjOperator(InstrOperator):
    """GetAdj 指令算子"""

    def incremental_execute(
        self, instr: Instruction, result: list[list[DynGraph]] = []
    ):
        """执行指令 (新逻辑: 增量边载入)"""

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

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        if USE_INCREMENTAL_EDGES_LOADING:
            return self.incremental_execute(instr, result)

        dbg.pprint_instr(instr)

        # 解析出, 当前的 `模式顶点`
        _, curr_pat_vid = self.resolve_var(instr.single_op)

        pattern_es = self.ctx.get_pattern_e_batch(instr.expand_eid_list)
        f_bucket = self.ctx.resolve_f_bucket(instr.single_op)
        A_bucket = A_Bucket.from_f_bucket(curr_pat_vid, f_bucket)

        # 先初始化 ctx 中 A_pool 对应位置
        self.ctx.init_A_pool(instr.target_var)

        # 按照 `下一个点的模式`, `分组` 加载边
        for pattern_e in pattern_es:
            label, attr = pattern_e.label, pattern_e.attr

            data_es = (
                self.storage_adapter.load_e(label)
                if not attr
                else self.storage_adapter.load_e_with_attr(label, attr)
            )

            if DIRECTED_EDGE_SUPPORT:
                # 如果支持有向边, 那么就要在 `src` 和 `dst` 上, 逐位进行匹配
                data_es = [
                    e
                    for e in data_es
                    if does_data_v_satisfy_pattern(
                        e.src_vid,
                        pattern_e.src_vid,
                        self.ctx.pattern_vs,
                        self.storage_adapter,
                    )
                    and does_data_v_satisfy_pattern(
                        e.dst_vid,
                        pattern_e.dst_vid,
                        self.ctx.pattern_vs,
                        self.storage_adapter,
                    )
                ]

            if not data_es:
                # 如果没有 `数据边`, 那么就应该 `放弃` 这一整次的 `扩张`
                # 因为这意味着无法匹配上当前的 `模式边`
                #
                # 放在这里是为了照顾 `DIRECTED_EDGE_SUPPORT` 为 `True` 的情况
                self.ctx.empty_matched_set_appeared = True
                return

            # 注意: next_pat_vid 不要直接根据 pattern_e 的终点加载
            next_pat_vid = (
                pattern_e.dst_vid
                if curr_pat_vid == pattern_e.src_vid
                else pattern_e.src_vid
            )

            A_bucket.with_new_edges_of_pattern(data_es, next_pat_vid, pattern_e)

        # A_bucket 过滤: 选出 `可连接的边` 和 `被连接的图`
        connected_data_vids = A_bucket.select_connective_edges_and_graphs(
            self.ctx.pattern_vs, self.storage_adapter
        )

        # 更新容器 (以及 `已被扩张的点集`)
        self.ctx.update_A_pool(instr.target_var, A_bucket)
        self.ctx.update_expanded_data_vids(connected_data_vids)
