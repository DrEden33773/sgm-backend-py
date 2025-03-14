from typing import TypedDict

from schema.basic import AttrType, EmptyDict, InstructionType, Op


class Attr(TypedDict):
    attr: str
    op: Op
    value: int | str
    type: AttrType


type AttrInfo = Attr | EmptyDict
""" ## 属性信息 = 属性字典 | 空字典 """
type VertexInfo = tuple[str, AttrInfo]
""" ### 标签, (属性) """
type EdgeInfo = tuple[str, str, str, AttrInfo]
""" ### 起点 vid, 终点 vid, 标签, (属性) """

type VertexInfoDict = dict[str, VertexInfo]
type EdgeInfoDict = dict[str, EdgeInfo]


class DisplayedInstr(TypedDict):
    vid: str
    type: InstructionType
    expand_eid_list: list[str]
    single_op: str
    multi_ops: list[str]
    target_var: str
    depend_on: list[str]


class PlanDict(TypedDict):
    matchingOrder: list[str]
    vertices: VertexInfoDict
    edges: EdgeInfoDict
    instructions: list[DisplayedInstr]
