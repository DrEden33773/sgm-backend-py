from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType


@OperatorFactory.register(InstructionType.GetAdj)
class GetAdjOperator(InstrOperator):
    """GetAdj 指令算子"""

    async def execute(self, instr: Instruction):
        """执行指令"""

        pg_es = self.ctx.get_pg_e_batch(instr.expand_eid_list)
        pg_e2v_s = self.ctx.get_pg_e2v_batch(pg_es)
        for pge, (src_v, dst_v) in zip(pg_es, pg_e2v_s):
            _expand_es = await self.storage_adapter.expand_edges_of(
                src_v_label=src_v.label,
                e_label=pge.label,
                dst_v_label=dst_v.label,
                src_v_attr=src_v.attr,
                e_attr=pge.attr,
                dst_v_attr=dst_v.attr,
            )

            # TODO: 更新容器 (扩张 `候选集`)
