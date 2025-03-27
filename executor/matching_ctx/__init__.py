from dataclasses import dataclass, field
from functools import lru_cache

from executor.matching_ctx.buckets import A_Bucket, C_Bucket, T_Bucket, f_Bucket
from executor.matching_ctx.type_aliases import DgVid, PgEid, PgVid
from schema import PlanData
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

    expanded_data_vids: set[DgVid] = field(default_factory=set)
    """ 已经在 GetAdj 步骤中, 被 expand 的数据图点集 """

    def update_expanded_data_vids(self, data_vids: set[DgVid]):
        """更新已连接数据图点集"""
        self.expanded_data_vids.update(data_vids)

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

    empty_matched_set_appeared: bool = False
    """ 
    是否出现了 `空匹配集`
    
    - 一旦出现, 意味着 GetAdj 过程因为无法完整匹配 `所有对应模式边` 而失败
    - 必须要放弃所有 f_pool 中残存的子图
    """

    def __post_init__(self):
        self.pattern_vs = self.plan_data.pattern_vs
        self.pattern_es = self.plan_data.pattern_es

    """ ========== """

    def init_f_pool(self, target_var: str):
        """Foreach: 初始化 f_pool"""
        key = resolve_var_name(target_var)
        self.F_pool[key] = f_Bucket()

    def append_to_f_pool(
        self,
        target_var: str,
        matched_dg: DynGraph,
        pivot: DgVid,
    ):
        """Init: 更新 f_pool 成功匹配部分"""
        key = resolve_var_name(target_var)
        next_idx = len(self.F_pool[key].all_matched)
        f_bucket = self.F_pool[key]
        f_bucket.all_matched.append(matched_dg)
        f_bucket.matched_with_pivots.setdefault(next_idx, []).append(pivot)

    def update_f_pool(self, target_var: str, f_bucket: f_Bucket):
        """Foreach: 更新 F_pool"""
        key = resolve_var_name(target_var)
        self.F_pool[key] = f_bucket

    def resolve_f_pool(self, single_op: str):
        """GetAdj: 解析 f_bucket"""
        key = resolve_var_name(single_op)
        return self.F_pool[key]

    def init_A_pool(self, target_var: str):
        """GetAdj: 初始化 A_pool"""
        key = resolve_var_name(target_var)
        self.A_pool[key] = A_Bucket(curr_pat_vid=key)

    def update_A_pool(self, target_var: str, a_bucket: A_Bucket):
        """GetAdj: 更新 A_pool"""
        key = resolve_var_name(target_var)
        self.A_pool[key] = a_bucket

    def resolve_A_pool(self, single_op: str):
        """Intersect(Ai): 解析 A_pool"""
        key = resolve_var_name(single_op)
        return self.A_pool[key]

    def init_C_pool(self, target_var: str):
        """Intersect: 初始化 C_pool"""
        key = resolve_var_name(target_var)
        self.C_pool[key] = C_Bucket()

    def update_C_pool(self, target_var: str, c_bucket: C_Bucket):
        """Intersect: 更新 C_pool"""
        key = resolve_var_name(target_var)
        self.C_pool[key] = c_bucket

    def resolve_C_pool(self, single_op: str):
        """Foreach: 解析 C_pool"""
        key = resolve_var_name(single_op)
        return self.C_pool[key]

    def init_T_pool(self, target_var: str):
        """Intersect: 初始化 T_pool"""
        key = resolve_var_name(target_var)
        self.T_pool[key] = T_Bucket(target_pat_vid=key)

    def update_T_pool(self, target_var: str, t_bucket: T_Bucket):
        """Intersect: 更新 T_pool"""
        key = resolve_var_name(target_var)
        self.T_pool[key] = t_bucket

    def resolve_T_pool(self, single_op: str):
        """Intersect(Tx): 解析 T_pool"""
        key = resolve_var_name(single_op)
        return self.T_pool[key]

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
