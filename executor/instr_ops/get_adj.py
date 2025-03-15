from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType


@OperatorFactory.register(InstructionType.GetAdj)
class GetAdjOperator(InstrOperator):
    """GetAdj 指令算子"""

    async def execute(self, instr: Instruction):
        """执行指令"""
