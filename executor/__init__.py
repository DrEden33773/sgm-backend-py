import json
from dataclasses import dataclass
from itertools import product
from typing import Iterable, cast

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

        unjoined_result: list[list[DynGraph]] = []
        instructions = self.plan_data.instructions
        operators: list[InstrOperator] = []

        for instr in instructions:
            instr_operator = OperatorFactory.create(
                instr, self.storage_adapter, self.matching_ctx
            )
            operators.append(instr_operator)

        assert len(operators) == len(instructions)
        for operator, instr in zip(operators, instructions):
            operator.execute(instr, unjoined_result)

        return unjoined_result

    def exec(self):
        unjoined = [partial for partial in self.exec_without_final_join() if partial]

        def preview_scale():
            len_list = [len(partial) for partial in unjoined]
            print(f"len_list = {len_list}")

        # dbg.pprint(unjoined)
        preview_scale()

        result: list[DynGraph] = []

        if not unjoined:
            return result

        plan_v_pat_cnt = {v_pat: 1 for v_pat in self.plan_data.pattern_vs}
        plan_e_pat_cnt = {e_pat: 1 for e_pat in self.plan_data.pattern_es}

        @staticmethod
        def could_match_the_whole_pattern(graph: DynGraph):
            graph_v_pat_cnt = {
                v_pat: len(vs) for v_pat, vs in graph.pattern_2_vs.items()
            }
            graph_e_pat_cnt = {
                e_pat: len(es) for e_pat, es in graph.pattern_2_es.items()
            }

            return (
                plan_v_pat_cnt == graph_v_pat_cnt and plan_e_pat_cnt == graph_e_pat_cnt
            )

        # 全排列组合
        for combination in product(*unjoined):
            curr = combination[0]
            for next in combination[1:]:
                new = curr | next
                curr = new
            result.append(curr)

        # 这里最后再过滤一遍, 合并好的图, 规模应该完全与 `pattern` 一致
        return list(graph for graph in result if could_match_the_whole_pattern(graph))

    @staticmethod
    def project_all_ids(merged_result: Iterable[DynGraph]):
        ids = [
            list(result.v_entities.keys()) + list(result.e_entities.keys())
            for result in merged_result
        ]
        print()
        dbg.pprint(ids)

    @staticmethod
    def dbg_deserialize_json(plan_json: str):
        plan_json_raw = json.loads(plan_json)
        plan_dict = cast(PlanDict, plan_json_raw)
        plan_data = PlanData.from_plan_dict(plan_dict)

        matching_order = plan_data.matching_order
        vertices = plan_data.pattern_vs
        edges = plan_data.pattern_es
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
