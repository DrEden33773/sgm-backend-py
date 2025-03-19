import json
from dataclasses import dataclass
from typing import cast

from executor.instr_ops.abc import InstrOperator
from executor.instr_ops.factory import OperatorFactory
from executor.matching_ctx import MatchingCtx
from schema import PlanData
from schema.json_repr_typed_dict import PlanDict
from storage.abc import StorageAdapter
from storage.sqlite import SQLiteStorageAdapter
from utils import dbg
from utils.dyn_graph import DynGraph


@dataclass
class ExecEngine:
    """执行引擎"""

    plan_data: PlanData
    matching_ctx: MatchingCtx
    storage_adapter: StorageAdapter

    @classmethod
    def from_json(
        cls, plan_json: str, storage_adapter: StorageAdapter = SQLiteStorageAdapter()
    ):
        plan_json_raw = json.loads(plan_json)
        plan_dict = cast(PlanDict, plan_json_raw)
        plan_data = PlanData.from_plan_dict(plan_dict)
        matching_ctx = MatchingCtx(plan_data)
        return cls(plan_data, matching_ctx, storage_adapter)

    def exec_without_final_join(self):
        """执行计划, 返回匹配结果 (有嵌套的返回, 并不执行最终的 Join)"""

        result: list[list[DynGraph]] = list()
        instructions = self.plan_data.instructions
        operators: list[InstrOperator] = list()

        for instr in instructions:
            instr_operator = OperatorFactory.create(
                instr, self.storage_adapter, self.matching_ctx
            )
            operators.append(instr_operator)

        assert len(operators) == len(instructions)
        for operator, instr in zip(operators, instructions):
            operator.execute(instr, result)

        return result

    @staticmethod
    def dbg_deserialize_json(plan_json: str):
        plan_json_raw = json.loads(plan_json)
        plan_dict = cast(PlanDict, plan_json_raw)
        plan_data = PlanData.from_plan_dict(plan_dict)

        matching_order = plan_data.matching_order
        vertices = plan_data.vertices
        edges = plan_data.edges
        instructions = plan_data.instructions

        dbg.print_header("Matching Order")
        dbg.pprint(matching_order)
        dbg.print_header("Vertices")
        dbg.pprint(vertices)
        dbg.print_header("Edges")
        dbg.pprint(edges)
        dbg.print_header("Instructions")
        dbg.pprint(instructions)

        return plan_dict
