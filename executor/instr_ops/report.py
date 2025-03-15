from typing import Any, Dict, List

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from schema import Instruction, InstructionType, Vid


@OperatorFactory.register(InstructionType.Report)
class ReportOperator(InstrOperator):
    """Report 指令算子"""

    async def execute(self, instr: Instruction):
        """执行指令"""

    def _collect_matches(
        self,
        results: List[Dict[str, Vid]],
        cur_match: Dict[str, Vid],
        var_names: List[str],
        idx: int,
        var_values: Dict[str, Any],
    ):
        """递归收集匹配结果"""
