from typing import TypedDict

from schema.basic import AttrType, InstructionType, Op


class Attr(TypedDict):
    """属性"""

    attr: str
    op: Op
    value: int | float | str
    type: AttrType


type Vid = str
""" 顶点 id 类型 """
type Eid = str
""" 边 id 类型 """
type Label = str
""" 标签类型  """
type EmptyDict = dict[str, str]
""" 空字典 """
type AttrInfo = Attr | EmptyDict
""" 属性信息 """
type VertexInfo = tuple[str, AttrInfo]
""" 标签, (属性信息) """
type EdgeInfo = tuple[str, str, str, AttrInfo]
""" 起点 vid, 终点 vid, 标签, (属性信息) """
type VertexInfoDict = dict[str, VertexInfo]
""" { vid -> 顶点信息 } """
type EdgeInfoDict = dict[str, EdgeInfo]
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

    matchingOrder: list[str]
    vertices: VertexInfoDict
    edges: EdgeInfoDict
    instructions: list[DisplayedInstr]
