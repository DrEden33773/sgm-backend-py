from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.matching_ctx.buckets import C_Bucket, T_Bucket
from schema import DataVertex, Instruction
from schema.basic import VarPrefix
from utils import dbg
from utils.dyn_graph import DynGraph


class IntersectOperator(InstrOperator):
    """Intersect 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        dbg.pprint_instr(instr)

        if instr.is_single_op():
            var_prefix, _ = self.resolve_var(instr.single_op)
            match var_prefix:
                case VarPrefix.DbQueryTarget:
                    self.with_adj_set(instr)
                case VarPrefix.IntersectTarget:
                    self.with_temp_intersected(instr)
                case _:
                    raise RuntimeError(f"Unexpected var_prefix: {var_prefix}")
        else:
            self.with_multi_adj_set(instr)

    def with_adj_set(self, instr: Instruction):
        """`Vi` ∩ `Ax` -> `Cy`"""

        # 初始化 ctx 中 C_pool 对应位置
        self.ctx.init_C_pool(instr.target_var)

        A_bucket = self.ctx.resolve_A_pool(instr.single_op or "")
        if not A_bucket:
            # 如果 A_bucket 为空, 说明没有邻接点, 那么就直接返回
            return

        loaded_vs, loaded_v_pat_strs = self.load_vertices(instr)
        C_bucket = C_Bucket.build_from_A(
            A_bucket,
            curr_pat_vid=instr.vid,
            loaded_vs=loaded_vs,
            loaded_v_pat_strs=loaded_v_pat_strs,
        )
        self.ctx.update_C_pool(instr.target_var, C_bucket)

    def with_multi_adj_set(self, instr: Instruction):
        """`A(T)_{i}` ∩ `A_{i+1}` -> `Tx`"""

        # 初始化 ctx 中 T_pool 对应位置
        self.ctx.init_T_pool(instr.target_var)

        A_buckets = [self.ctx.resolve_A_pool(op) for op in instr.multi_ops]
        A1, A2 = A_buckets
        if not A1 or not A2:
            return

        T_bucket = T_Bucket.build_from_A_A(A1, A2, instr.vid)
        if len(A_buckets) > 2:
            prev_T = T_bucket
            for A_bucket in A_buckets[2:]:
                if not A_bucket:
                    return
                T_bucket = T_Bucket.build_from_T_A(prev_T, A_bucket)
                prev_T = T_bucket

            if not prev_T:
                return
            T_bucket = prev_T

        self.ctx.update_T_pool(instr.target_var, T_bucket)

    def with_temp_intersected(self, instr: Instruction):
        """`Vi` ∩ `Tx` -> `Cy`"""

        # 初始化 ctx 中 C_pool 对应位置
        self.ctx.init_C_pool(instr.target_var)

        loaded_vs, loaded_v_pat_strs = self.load_vertices(instr)
        T_bucket = self.ctx.resolve_T_pool(instr.single_op or "")
        if not T_Bucket:
            return

        C_bucket = C_Bucket.build_from_T(T_bucket, loaded_vs, loaded_v_pat_strs)
        self.ctx.update_C_pool(instr.target_var, C_bucket)

    """ ========== Helpers ========== """

    def load_vertices(self, instr: Instruction):
        pattern_v = self.ctx.get_pattern_v(instr.vid)
        label, attr = pattern_v.label, pattern_v.attr
        loaded_vs: list[DataVertex] = []
        if not attr:
            loaded_vs = self.storage_adapter.load_v(label)
        else:
            loaded_vs = self.storage_adapter.load_v_with_attr(label, attr)
        loaded_v_pat_strs = [pattern_v.vid for _ in range(len(loaded_vs))]
        return loaded_vs, loaded_v_pat_strs
