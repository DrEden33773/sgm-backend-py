import json
from typing import cast

from schema import PlanDict
from utils import dbg


class ExecEngine:
    def __init__(self) -> None:
        pass

    @staticmethod
    def deserialize_json(plan_json: str):
        plan_data = json.loads(plan_json)
        plan = cast(PlanDict, plan_data)

        matching_order = plan.get("matchingOrder", [])
        vertices = plan.get("vertices", {})
        edges = plan.get("edges", {})
        instructions = plan.get("instructions", [])

        dbg.print_header("Matching Order")
        dbg.pprint(matching_order)
        dbg.print_header("Vertices")
        dbg.pprint(vertices)
        dbg.print_header("Edges")
        dbg.pprint(edges)
        dbg.print_header("Instructions")
        dbg.pprint(instructions)

        return plan
