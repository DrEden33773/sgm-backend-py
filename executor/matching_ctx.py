from dataclasses import dataclass, field

from schema import Edge, Eid, PlanData, Vertex, Vid
from utils.dyn_graph import DynGraph

type PgVid = Vid
""" 模式图 - Vid """
type PgEid = Eid
""" 模式图 - Eid """
type DgVid = Vid
""" 数据图 - Vid """
type DgEid = Eid
""" 数据图 - Eid """

type PartialMatched = DynGraph
""" 部分匹配 """


@dataclass
class MatchingCtx:
    """核心组件: 上下文"""

    plan_data = PlanData
    """ 执行计划 """

    pg_v_entities: dict[PgVid, Vertex] = field(default_factory=dict)
    """
    模式图
    - { vid -> Vertex }
    """

    pg_e_entities: dict[PgEid, Edge] = field(default_factory=dict)
    """ 
    模式图
    - { eid -> Edge }
    """

    bucket: dict[PgVid, dict[DgVid, list[PartialMatched]]] = field(default_factory=dict)
    """ 
    容器
    - { pg_vid -> { dg_vid -> partial_matched } }
    - { `块标签 pg_vid` -> { `基准 dg_vid` -> `部分匹配集` } }
    """

    var_dict: dict[str, PgVid] = field(default_factory=dict)
    """
    变量字典 
    - { 变量名 -> pg_vid }
    - pg_vid 用于在 `bucket` 中查找对应的 `部分匹配块`
    """

    def __post_init__(self):
        self.pg_v_entities = self.plan_data.vertices
        self.pg_e_entities = self.plan_data.edges

    def get_pg_v(self, vid: PgVid):
        """获取模式图顶点"""

        return self.pg_v_entities[vid]

    def get_pg_v_batch(self, vids: list[PgVid]):
        """批量获取模式图顶点"""

        return [self.pg_v_entities[vid] for vid in vids]

    def get_pg_e(self, eid: PgEid):
        """获取模式图边"""

        return self.pg_e_entities[eid]

    def get_pg_e_batch(self, eids: list[PgEid]):
        """批量获取模式图边"""

        return [self.pg_e_entities[eid] for eid in eids]

    def get_pg_e_2v(self, e: PgEid | Edge):
        """获取模式图边的两个顶点"""

        eid = e.eid if isinstance(e, Edge) else e
        src, dst = self.pg_e_entities[eid].src_vid, self.pg_e_entities[eid].dst_vid
        return self.pg_v_entities[src], self.pg_v_entities[dst]

    def get_pg_e2v_batch(self, eids: list[PgEid] | list[Edge]):
        """批量获取模式图边的两个顶点"""

        return [self.get_pg_e_2v(eid) for eid in eids]

    def update_var(self, var_name: str, vid: PgVid):
        """更新变量"""

        self.var_dict[var_name] = vid
