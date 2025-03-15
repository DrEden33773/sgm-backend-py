from dataclasses import dataclass, field

from schema import Eid, PlanData, Vid
from utils.dyn_graph import DynGraph

type PgVid = Vid
""" 模式图 - Vid """
type PgEid = Eid
""" 模式图 - Eid """
type DgVid = Vid
""" 数据图 - Vid """
type DgEid = Eid
""" 数据图 - Eid """


@dataclass
class MatchingCtx:
    """核心组件: 上下文"""

    plan_data = PlanData
    """ 执行计划 """

    bucket: dict[PgVid, DynGraph] = field(default_factory=dict)
