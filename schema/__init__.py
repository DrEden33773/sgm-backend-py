from dataclasses import dataclass
from typing import Optional

from schema.basic import AttrType, InstructionType, Op
from schema.json_repr_typed_dict import (
    AttrInfo,
    DisplayedInstr,
    EdgeInfoTuple,
    PlanDict,
    VertexInfoTuple,
)

type Vid = str
""" 顶点 id 类型 """
type Eid = str
""" 边 id 类型 """
type Label = str
""" 标签类型  """


@dataclass
class Attr:
    attr: str
    op: Op
    value: int | float | str
    type: AttrType

    @classmethod
    def from_attr_info(cls, info: AttrInfo):
        attr = info.get("attr", None)
        op = info.get("op", None)
        value = info.get("value", None)
        type = info.get("type", None)
        if any(v is None for v in (attr, op, value, type)):
            return None
        return cls(attr, op, value, type)


@dataclass
class Instruction:
    """指令"""

    vid: str
    type: InstructionType
    expand_eid_list: list[str]
    single_op: str
    multi_ops: list[str]
    target_var: str
    depend_on: list[str]

    @classmethod
    def from_displayed_instr(cls, info: DisplayedInstr):
        return cls(**info)


@dataclass
class Vertex:
    """顶点信息"""

    vid: Vid
    label: Label
    attr: Optional[Attr] = None

    @classmethod
    def from_vertex_info(cls, vid: Vid, info: VertexInfoTuple):
        label, attr = info
        return cls(vid, label, Attr.from_attr_info(attr))


@dataclass
class Edge:
    """边信息"""

    eid: Eid
    label: Label
    src_vid: Vid
    dst_vid: Vid
    attr: Optional[Attr] = None

    @classmethod
    def from_edge_info(cls, eid: Eid, info: EdgeInfoTuple):
        src_vid, dst_vid, label, attr = info
        return cls(eid, label, src_vid, dst_vid, Attr.from_attr_info(attr))


@dataclass
class PlanData:
    """执行计划 (详细数据)"""

    matching_order: list[str]
    vertices: dict[Vid, Vertex]
    edges: dict[Eid, Edge]
    instructions: list[Instruction]

    @classmethod
    def from_plan_dict(cls, plan_dict: PlanDict):
        matching_order = plan_dict.get("matching_order", [])
        vertices = {
            vid: Vertex.from_vertex_info(vid, info)
            for vid, info in plan_dict.get("vertices", {}).items()
        }
        edges = {
            eid: Edge.from_edge_info(eid, info)
            for eid, info in plan_dict.get("edges", {}).items()
        }
        instructions = [
            Instruction.from_displayed_instr(info)
            for info in plan_dict.get("instructions", [])
        ]
        return cls(matching_order, vertices, edges, instructions)
