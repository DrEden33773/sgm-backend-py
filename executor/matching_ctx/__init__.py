from dataclasses import dataclass, field
from functools import lru_cache

from executor.matching_ctx.buckets import A_Bucket, C_Bucket, T_Bucket, f_Bucket
from executor.matching_ctx.type_aliases import PgEid, PgVid
from schema import DataEdge, PatternEdge, PatternVertex, PlanData
from schema.basic import STR_TUPLE_SPLITTER
from utils.dyn_graph import DynGraph


@lru_cache
def resolve_var_name(target_var: str):
    """解析变量名"""
    return target_var.split(STR_TUPLE_SPLITTER)[1]


@dataclass
class MatchingCtx:
    """核心组件: 上下文"""

    plan_data: PlanData
    """ 执行计划 """

    pattern_vs: dict[PgVid, PatternVertex] = field(default_factory=dict)
    """ 模式图点集 """

    pattern_es: dict[PgEid, PatternEdge] = field(default_factory=dict)
    """ 模式图边集 """

    F_pool: dict[PgVid, f_Bucket] = field(default_factory=dict)
    """
    枚举目标 (f) 容器
    - { 点模式标签 pg_vid -> f_bucket } 
    """

    A_pool: dict[PgVid, A_Bucket] = field(default_factory=dict)
    """
    邻接组合 (A) 容器
    - { 点模式标签 pg_vid -> A_bucket }
    """

    C_pool: dict[PgVid, C_Bucket] = field(default_factory=dict)
    """ 
    候选集 (C) 容器
    - { 点模式标签 pg_vid -> C_bucket }
    """

    T_pool: dict[PgVid, T_Bucket] = field(default_factory=dict)
    """ 
    临时交集 (T) 容器
    - { 点模式标签 pg_vid -> T_bucket }
    """

    def __post_init__(self):
        self.pattern_vs = self.plan_data.vertices
        self.pattern_es = self.plan_data.edges

    """ ========== """

    def init_f_pool(self, target_var: str):
        """Init: 初始化 f_pool"""
        key = resolve_var_name(target_var)
        self.F_pool[key] = f_Bucket()

    def append_to_f_pool(self, target_var: str, matched_dg: DynGraph):
        """Init: 更新 f_pool 成功匹配部分"""
        key = resolve_var_name(target_var)
        self.F_pool[key].append_matched(matched_dg)

    def resolve_f_bucket(self, single_op: str):
        """GetAdj: 解析 f_bucket"""
        key = resolve_var_name(single_op)
        return self.F_pool[key]

    def update_A_pool(self, target_var: str, a_bucket: A_Bucket):
        """GetAdj: 更新 A_pool"""
        key = resolve_var_name(target_var)
        self.A_pool[key] = a_bucket

    def resolve_A_pool(self, single_op: str):
        """Intersect(Ai): 解析 A_pool"""
        key = resolve_var_name(single_op)
        return self.A_pool[key]

    def update_C_pool(self, target_var: str, c_bucket: C_Bucket):
        """Intersect: 更新 C_pool"""
        key = resolve_var_name(target_var)
        self.C_pool[key] = c_bucket

    def resolve_T_pool(self, single_op: str):
        """Intersect(Tx): 解析 T_pool"""
        key = resolve_var_name(single_op)
        return self.T_pool[key]

    def update_T_pool(self, target_var: str, t_bucket: T_Bucket):
        """Intersect: 更新 T_pool"""
        key = resolve_var_name(target_var)
        self.T_pool[key] = t_bucket

    def resolve_C_pool(self, single_op: str):
        """Foreach: 解析 C_pool"""
        key = resolve_var_name(single_op)
        return self.C_pool[key]

    def update_f_pool(self, target_var: str, f_bucket: f_Bucket):
        """Foreach: 更新 F_pool"""
        key = resolve_var_name(target_var)
        self.F_pool[key] = f_bucket

    """ ========== """

    def get_pattern_v(self, vid: PgVid):
        """获取模式图顶点"""
        return self.pattern_vs[vid]

    def get_pattern_v_batch(self, vids: list[PgVid]):
        """批量获取模式图顶点"""
        return [self.pattern_vs[vid] for vid in vids]

    def get_pattern_e(self, eid: PgEid):
        """获取模式图边"""
        return self.pattern_es[eid]

    def get_pattern_e_batch(self, eids: list[PgEid]):
        """批量获取模式图边"""
        return [self.pattern_es[eid] for eid in eids]

    def get_pattern_e2v(self, e: DataEdge):
        """获取模式图边的两个顶点"""
        src, dst = self.pattern_es[e.eid].src_vid, self.pattern_es[e.eid].dst_vid
        return self.pattern_vs[src], self.pattern_vs[dst]

    def get_pattern_e2v_batch(self, es: list[DataEdge]):
        """批量获取模式图边的两个顶点"""
        return [self.get_pattern_e2v(eid) for eid in es]
