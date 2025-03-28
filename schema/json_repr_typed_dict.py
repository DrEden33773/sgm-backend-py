from typing import Optional, TypedDict

from schema.basic import AttrType, InstructionType, Op

type Vid = str
""" 顶点 id 类型 """
type Eid = str
""" 边 id 类型 """
type Label = str
""" 标签类型  """


class DisplayedAttr(TypedDict):
    """属性"""

    attr: str
    op: Op
    value: int | float | str
    type: AttrType


type AttrInfo = Optional[DisplayedAttr]
""" 属性信息 """


class VInfo(TypedDict):
    vid: Vid
    label: Label
    attr: AttrInfo


class EInfo(TypedDict):
    eid: Eid
    label: Label
    src_vid: Vid
    dst_vid: Vid
    attr: AttrInfo


class DisplayedInstr(TypedDict):
    """可序列化-指令"""

    vid: str
    type: InstructionType
    expand_eid_list: list[str]
    single_op: Optional[str]
    multi_ops: list[str]
    target_var: str
    depend_on: list[str]


class PlanDict(TypedDict):
    """执行计划"""

    matching_order: list[str]
    vertices: dict[Vid, VInfo]
    edges: dict[Eid, EInfo]
    instructions: list[DisplayedInstr]
