from copy import deepcopy
from dataclasses import dataclass, field

from executor.matching_ctx.type_aliases import DgEdge, DgEid, PgEid, PgVid
from schema import DataVertex
from utils.dyn_graph import DynGraph
from utils.expanding_graph import ExpandGraph


@dataclass
class f_Bucket:
    """枚举目标 (f) 桶"""

    all_matched: list[DynGraph] = field(default_factory=list)

    def __post_init__(self):
        self.append_matched = self.all_matched.append

    @classmethod
    def from_C_bucket(cls, C_bucket: "C_Bucket"):
        all_matched: list[DynGraph] = []

        # 无需 `分裂式` 枚举, 直接导出
        if not C_bucket.need_further_enumeration:
            all_matched = [g.to_dyn_graph_emplace() for g in C_bucket.all_expanded]
            return cls(all_matched)

        # 需要 `分裂式` 枚举
        for expanded in C_bucket.all_expanded:
            target_v = expanded.target_v_entities.values()
            for v in target_v:
                # 这里得用 deepcopy (cloned), 因为每个 target_v 都会生成一张图
                new_dg = expanded.to_dyn_graph_cloned()
                new_dg.update_v(v)
                all_matched.append(new_dg)

        return cls(all_matched)


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
    def from_f_bucket(cls, f_bucket: f_Bucket):
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


@dataclass
class C_Bucket:
    """候选集 (C) 桶"""

    all_expanded: list[ExpandGraph] = field(default_factory=list)
    need_further_enumeration: bool = True

    @classmethod
    def build_from_A(
        cls, A_bucket: A_Bucket, curr_pat_vid: PgVid, loaded_vertices: list[DataVertex]
    ):
        # 从 A_bucket 中弹出当前分组
        curr_group = A_bucket.dst_pat_grouped_expanding.pop(curr_pat_vid, [])
        if not curr_group:
            return cls()

        all_expanded: list[ExpandGraph] = []

        for expanding in curr_group:
            # 与给定点集 `loaded_vertices` 求交
            expanding.update_available_targets(loaded_vertices)

            # 更新 `all_expanded`
            all_expanded.append(expanding)

        # 从 `A_bucket` 构造的图, 后续还需要 `进一步枚举`
        return cls(all_expanded, need_further_enumeration=True)

    @classmethod
    def build_from_T(cls, T_bucket: "T_Bucket", loaded_vertices: list[DataVertex]):
        if not T_bucket.expanding_graphs:
            return cls()

        all_expanded: list[ExpandGraph] = []

        for expanding in T_bucket.expanding_graphs:
            # 与给定点集 `loaded_vertices` 求交
            expanding.update_available_targets(loaded_vertices)

            # 更新 `all_expanded`
            all_expanded.append(expanding)

        # 从 `T_bucket` 构造的图, 已经被 `完全枚举`
        return cls(all_expanded, need_further_enumeration=False)


@dataclass
class T_Bucket:
    """
    临时交集 (T) 桶

    - 实际上非常像 `A` 桶
        - 准确的说, 是 `A` 桶的一个子集
    """

    target_pat_vid: PgVid

    expanding_graphs: list[ExpandGraph] = field(default_factory=list)

    @classmethod
    def build_from_A_A(cls, left: A_Bucket, right: A_Bucket, target_pat_vid: PgVid):
        left_group = left.dst_pat_grouped_expanding.pop(target_pat_vid, [])
        right_group = right.dst_pat_grouped_expanding.pop(target_pat_vid, [])
        expanding_graphs = cls.expand_edges_on_same_vertices(left_group, right_group)
        return cls(target_pat_vid, expanding_graphs)

    @classmethod
    def build_from_T_A(cls, left: "T_Bucket", right: A_Bucket):
        left_group = left.expanding_graphs
        right_group = right.dst_pat_grouped_expanding.pop(left.target_pat_vid, [])

        # 事实上, 这里可以肯定地说, `T_Bucket` 就是不完整的, 而 `A_Bucket` 就是未被利用的
        # 所以显式指定参数位置
        expanding_graphs = cls.expand_edges_on_same_vertices(
            potential_incomplete_group=left_group, potential_unused_group=right_group
        )
        return cls(left.target_pat_vid, expanding_graphs)

    @staticmethod
    def expand_edges_on_same_vertices(
        potential_unused_group: list[ExpandGraph],
        potential_incomplete_group: list[ExpandGraph],
    ):
        """
        ## 在共同点上, 逐位置扩张边

        ### 目的

        把 `未被利用的` 垂悬边, 尝试连接在 `更年轻` 的图上

        ### 说明

        更短的列表中的图, 经由 `Foreach` 迭代的次数 `更少`

        不难发现: `Foreach` 的迭代次数, 正比于, 对应 `依赖变量` 的年龄

        也就说明, 其中未被 `利用 (连接)` 的 `垂悬边` 存在的概率更大
        """

        result: list[ExpandGraph] = []

        outer_, inner_ = potential_unused_group, potential_incomplete_group
        if len(outer_) > len(inner_):
            outer_, inner_ = inner_, outer_

        for outer in outer_:
            for inner in inner_:
                if not (outer.get_vid_set() & inner.get_vid_set()):
                    # 先在 `点` 上取交集
                    continue

                # 虽然理论上来说 outer 是 unused 概率更大, 但是还是要严格的判断
                # 判断逻辑: 谁是子集, 谁就是 unused
                unused, incomplete = outer, inner
                if not outer.get_vid_set() <= inner.get_vid_set():
                    unused, incomplete = inner, outer

                # 再对共同点的 `边`, 取并集, 进行扩张
                # 注意, 这里会按照 `unused` 边的数量, 拷贝多份 `incomplete` 图
                # 每份 incomplete 图, 都会连接一条 unused 边
                unions = ExpandGraph.intersect_then_union_on_same_v(unused, incomplete)
                result.extend(unions)

        return result
