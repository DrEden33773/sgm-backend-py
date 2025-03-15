from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType


@OperatorFactory.register(InstructionType.Foreach)
class ForeachOperator(InstrOperator):
    """Foreach 指令算子"""

    async def execute(self, instr: Instruction):
        """执行指令"""
