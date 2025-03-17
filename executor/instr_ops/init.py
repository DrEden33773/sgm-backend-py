from typing import override

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType
from utils.dyn_graph import DynGraph


@OperatorFactory.register(InstructionType.Init)
class InitOperator(InstrOperator):
    """
    Init 指令算子

    - 结果类型: f (EnumerateTarget)
    """

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = list()):
        """执行指令"""

        pattern_v = self.ctx.get_pattern_v(instr.vid)
        label, attr = pattern_v.label, pattern_v.attr

        # 加载顶点
        matched_vs = self.storage_adapter.load_vertices(label, attr)

        # 更新容器
        for dg_v in matched_vs:
            matched_dg = DynGraph().update_v(dg_v)
            self.ctx.append_to_f_pool(instr.target_var, matched_dg)
