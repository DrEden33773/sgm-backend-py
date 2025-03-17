from copy import deepcopy
from dataclasses import dataclass, field

from executor.matching_ctx.buckets.f_bucket import F_Bucket
from executor.matching_ctx.type_aliases import DgEdge, DgEid, PgEid
from utils.dyn_graph import DynGraph
from utils.expanding_graph import ExpandGraph


@dataclass
class A_Bucket:
    """邻接组合 (A) 桶"""

    all_matched: list[DynGraph] = field(default_factory=list)
    appeared_eids: set[DgEid] = field(default_factory=set)

    dst_pat_grouped_edges: dict[PgEid, list[DgEdge]] = field(default_factory=dict)
    dst_pat_grouped_expanding: dict[PgEid, list[ExpandGraph]] = field(
        default_factory=dict
    )

    @classmethod
    def from_f_bucket(cls, f_bucket: F_Bucket):
        return cls(f_bucket.all_matched)

    def __post_init__(self):
        # 立刻更新 `appeared_eids`
        for dg in self.all_matched:
            self.appeared_eids.update(dg.e_entities.keys())

    def with_new_edges(self, new_edges: list[DgEdge], dst_pat_vid: PgEid):
        """添加新边 (不在 incoming_eids 中)"""

        valid_new_edges = [e for e in new_edges if e.eid not in self.appeared_eids]
        self.dst_pat_grouped_edges.setdefault(dst_pat_vid, []).extend(valid_new_edges)

        # 记得更新 `appeared_eids`
        self.appeared_eids.update(e.eid for e in valid_new_edges)

        return self

    def select_connective_edges_and_graphs(self):
        """选出 `可连接的边` 和 `被连接的图` (内部求交)"""

        if not self.all_matched or not self.dst_pat_grouped_edges:
            return

        # 迭代 `已匹配` 的数据图
        for dg in self.all_matched:
            # 分组迭代 `新增边`
            for dst_pat, edges in self.dst_pat_grouped_edges.items():
                # 挑选出 `可连接边`
                connective_edges = [
                    edge for edge in edges if dg.is_edge_connective(edge)
                ]
                # 指定位置, 构造 `扩展图`
                new_dg = deepcopy(dg).update_e_batch(connective_edges)
                self.dst_pat_grouped_expanding.setdefault(dst_pat, []).append(
                    ExpandGraph(new_dg)
                )

        self.all_matched.clear()
        self.dst_pat_grouped_edges.clear()
