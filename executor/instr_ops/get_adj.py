from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from executor.matching_ctx import A_Bucket
from schema import Instruction, InstructionType
from utils.dyn_graph import DynGraph


@OperatorFactory.register(InstructionType.GetAdj)
class GetAdjOperator(InstrOperator):
    """GetAdj 指令算子"""

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""

        pattern_es = self.ctx.get_pattern_e_batch(instr.expand_eid_list)
        f_bucket = self.ctx.resolve_f_bucket(instr.single_op)
        A_bucket = A_Bucket.from_f_bucket(f_bucket)

        # 按照 `终点的模式`, `分组` 加载边
        for pattern_e in pattern_es:
            label, attr = pattern_e.label, pattern_e.attr
            data_es = self.storage_adapter.load_edges(label, attr)
            A_bucket.with_new_edges(data_es, dst_pat_vid=pattern_e.dst_vid)

        # A_bucket 过滤: 选出 `可连接的边` 和 `被连接的图`
        A_bucket.select_connective_edges_and_graphs()

        # 更新容器
        self.ctx.update_A_pool(instr.target_var, A_bucket)
