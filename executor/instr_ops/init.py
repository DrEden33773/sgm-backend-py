from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType
from utils.dyn_graph import DynGraph


@OperatorFactory.register(InstructionType.Init)
class InitOperator(InstrOperator):
    """Init 指令算子"""

    async def execute(self, instr: Instruction):
        """执行指令"""

        pg_v = self.ctx.get_pg_v(instr.vid)
        label, attr = pg_v.label, pg_v.attr

        # 加载顶点
        matched_vs = await self.storage_adapter.load_vertices(label, attr)

        # 更新容器
        for dgv in matched_vs:
            dgv_Cs = self.ctx.bucket.setdefault(instr.vid, {})
            Cs = dgv_Cs.setdefault(dgv.vid, [])

            partial_matched = DynGraph().update_v(dgv)
            Cs.append(partial_matched)

        # 更新变量
        self.ctx.update_var(instr.target_var, instr.vid)
