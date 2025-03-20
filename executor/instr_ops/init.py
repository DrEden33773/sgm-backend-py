from typing import override

from executor.instr_ops.abc import InstrOperator
from schema import Instruction
from utils import dbg
from utils.dyn_graph import DynGraph


class InitOperator(InstrOperator):
    """
    Init 指令算子

    - 结果类型: f (EnumerateTarget)
    """

    @override
    def execute(self, instr: Instruction, result: list[list[DynGraph]] = []):
        """执行指令"""

        dbg.pprint_instr(instr)

        pattern_v = self.ctx.get_pattern_v(instr.vid)
        label, attr = pattern_v.label, pattern_v.attr

        # 加载顶点
        matched_vs = (
            self.storage_adapter.load_v(label)
            if not attr
            else self.storage_adapter.load_v_with_attr(label, attr)
        )

        # 这里一定先 `初始化` 容器
        self.ctx.init_f_pool(instr.target_var)

        # 更新容器
        for dg_v in matched_vs:
            matched_dg = DynGraph().update_v(dg_v)
            self.ctx.append_to_f_pool(instr.target_var, matched_dg)
