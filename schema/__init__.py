from dataclasses import dataclass, field
from typing import Optional

from schema.basic import (
    STR_TUPLE_SPLITTER,
    AttrType,
    InstructionType,
    Op,
    VarPrefix,
    str_op_to_operator,
)
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
class PatternAttr:
    key: str
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

    def __hash__(self) -> int:
        return hash(self.key)

    def is_data_attr_satisfied(self, data_attr: Optional[int | float | str] = None):
        operator = str_op_to_operator(self.op)
        if not data_attr:
            return False
        if type(data_attr) is not type(self.value):
            return False
        return operator(data_attr, self.value)

    def is_data_attrs_satisfied(self, data_attrs: dict[str, int | float | str]):
        if self.key not in data_attrs:
            return False
        return self.is_data_attr_satisfied(data_attrs[self.key])


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

    def is_single_op(self) -> bool:
        """是否为 `单输入变量` 操作"""

        return bool(self.single_op)

    def resolve_vid_from_target_var(self):
        """从 `target_var` 解析 vid"""

        if self.target_var == VarPrefix.DataVertexSet:
            return ""
        return self.target_var.split(STR_TUPLE_SPLITTER)[1]

    def resolve_vids_from_multi_ops(self):
        """从 `multi_ops` 解析 vids"""

        vids: list[str] = []
        for op in self.multi_ops:
            if op == VarPrefix.DataVertexSet:
                vids.append("")
            else:
                vids.append(op.split(STR_TUPLE_SPLITTER)[1])
        return vids


@dataclass
class VertexBase:
    vid: Vid
    label: Label


@dataclass
class EdgeBase:
    eid: Eid
    label: Label
    src_vid: Vid
    dst_vid: Vid

    def __contains__(self, vid: Vid):
        return vid in (self.src_vid, self.dst_vid)

    def reversed(self):
        self.src_vid, self.dst_vid = self.dst_vid, self.src_vid
        return self


@dataclass
class PatternVertex(VertexBase):
    """模式顶点信息"""

    attr: Optional[PatternAttr] = None

    @classmethod
    def from_vertex_info(cls, vid: Vid, info: VertexInfoTuple):
        label, attr = info
        return cls(vid, label, PatternAttr.from_attr_info(attr))

    def __hash__(self) -> int:
        return hash(self.vid)


@dataclass
class DataVertex(VertexBase):
    """数据顶点"""

    attrs: dict[str, int | float | str] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.vid)


@dataclass
class PatternEdge(EdgeBase):
    """模式边信息"""

    attr: Optional[PatternAttr] = None

    @classmethod
    def from_edge_info(cls, eid: Eid, info: EdgeInfoTuple):
        src_vid, dst_vid, label, attr = info
        return cls(eid, label, src_vid, dst_vid, PatternAttr.from_attr_info(attr))

    def __hash__(self) -> int:
        return hash(self.eid)


@dataclass
class DataEdge(EdgeBase):
    """数据边"""

    attrs: dict[str, int | float | str] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.eid)


@dataclass
class PlanData:
    """执行计划 (详细数据)"""

    matching_order: list[str]
    vertices: dict[Vid, PatternVertex]
    edges: dict[Eid, PatternEdge]
    instructions: list[Instruction]

    @classmethod
    def from_plan_dict(cls, plan_dict: PlanDict):
        matching_order = plan_dict.get("matching_order", [])
        vertices = {
            vid: PatternVertex.from_vertex_info(vid, info)
            for vid, info in plan_dict.get("vertices", {}).items()
        }
        edges = {
            eid: PatternEdge.from_edge_info(eid, info)
            for eid, info in plan_dict.get("edges", {}).items()
        }
        instructions = [
            Instruction.from_displayed_instr(info)
            for info in plan_dict.get("instructions", [])
        ]
        return cls(matching_order, vertices, edges, instructions)
