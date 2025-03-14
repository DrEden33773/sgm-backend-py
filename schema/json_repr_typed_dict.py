from typing import Any, TypedDict

from schema.basic import AttrType, InstructionType, Op


class DisplayedAttr(TypedDict):
    """属性"""

    attr: str
    op: Op
    value: int | float | str
    type: AttrType


type EmptyDict = dict[str, Any]
""" 空字典 """
type AttrInfo = DisplayedAttr | EmptyDict
""" 属性信息 """
type VertexInfoTuple = tuple[str, AttrInfo]
""" 标签, (属性信息) """
type EdgeInfoTuple = tuple[str, str, str, AttrInfo]
""" 起点 vid, 终点 vid, 标签, (属性信息) """
type VertexInfoDict = dict[str, VertexInfoTuple]
""" { vid -> 顶点信息 } """
type EdgeInfoDict = dict[str, EdgeInfoTuple]
""" { eid -> 边信息 } """


class DisplayedInstr(TypedDict):
    """可序列化-指令"""

    vid: str
    type: InstructionType
    expand_eid_list: list[str]
    single_op: str
    multi_ops: list[str]
    target_var: str
    depend_on: list[str]


class PlanDict(TypedDict):
    """执行计划"""

    matching_order: list[str]
    vertices: VertexInfoDict
    edges: EdgeInfoDict
    instructions: list[DisplayedInstr]
