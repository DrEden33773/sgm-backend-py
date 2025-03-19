from copy import deepcopy
from dataclasses import dataclass, field

from executor.matching_ctx.type_aliases import DgEdge, DgVid, PgEid, PgVid
from schema import DataEdge, DataVertex, PatternVertex
from storage.abc import StorageAdapter
from utils.dyn_graph import DynGraph
from utils.expanding_graph import ExpandGraph


def does_data_v_satisfy_pattern(
    dg_vid: DgVid,
    pat_vid: PgEid,
    pattern_vs: dict[PgVid, PatternVertex],
    storage_adapter: StorageAdapter,
) -> bool:
    pattern_v = pattern_vs[pat_vid]
    data_v = storage_adapter.get_v(dg_vid)

    # 比较标签
    if pattern_v.label != data_v.label:
        return False

    # 比较属性
    if not pattern_v.attr:
        # 模式点未规定属性
        return True
    elif not data_v.attr:
        # 模式点规定了属性, 但是数据点没有属性
        return False
    else:
        operator = pattern_v.attr.op.to_operator()
        data_v_attr_value = data_v.attr
        pattern_v_attr_value = pattern_v.attr.value
        if type(data_v_attr_value) is not type(pattern_v_attr_value):
            return False
        return operator(data_v_attr_value, pattern_v_attr_value)


@dataclass
class f_Bucket:
    """枚举目标 (f) 桶"""

    all_matched: list[DynGraph] = field(default_factory=list)

    def __post_init__(self):
        self.append_matched = self.all_matched.append

    @classmethod
    def from_C_bucket(cls, C_bucket: "C_Bucket"):
        all_matched: list[DynGraph] = []

        # if not C_bucket.need_further_enumeration:
        #     all_matched = [g.to_dyn_graph_cloned() for g in C_bucket.all_expanded]
        #     return cls(all_matched)
        # for expanded in C_bucket.all_expanded:
        #     target_v = expanded.target_v_entities.values()
        #     for v in target_v:
        #         # 这里得用 deepcopy (cloned), 因为每个 target_v 都会生成一张图
        #         new_dg = expanded.to_dyn_graph_cloned()
        #         new_dg.update_v(v)
        #         all_matched.append(new_dg)
        # return cls(all_matched)

        # 现在的算法, 会在 C_bucket 阶段, 直接完成基于 `下一个数据点` 的 `分裂`
        all_matched = [g.to_dyn_graph_cloned() for g in C_bucket.all_expanded]
        return cls(all_matched)


@dataclass
class A_Bucket:
    """邻接组合 (A) 桶"""

    curr_pat_vid: PgVid
    all_matched: list[DynGraph] = field(default_factory=list)

    next_pat_grouped_edges: dict[PgVid, set[DgEdge]] = field(default_factory=dict)
    next_pat_grouped_expanding: dict[PgVid, list[ExpandGraph]] = field(
        default_factory=dict
    )

    @classmethod
    def from_f_bucket(cls, curr_pat_vid: PgVid, f_bucket: f_Bucket):
        return cls(curr_pat_vid, f_bucket.all_matched)

    def with_new_edges(self, new_edges: list[DgEdge], next_pat_vid: PgEid):
        """添加新边"""

        curr_group = self.next_pat_grouped_edges.setdefault(next_pat_vid, set())
        curr_group.update(new_edges)
        return self

    def select_connective_edges_and_graphs(
        self,
        pattern_vs: dict[PgVid, PatternVertex],
        storage_adapter: StorageAdapter,
    ):
        """选出 `可连接的边` 和 `被连接的图` (内部求交)"""

        if not self.all_matched or not self.next_pat_grouped_edges:
            return

        # 迭代 `已匹配` 的数据图
        for dg in self.all_matched:
            is_curr_dg_expandable = False

            # 分组迭代 `新增边`
            for next_pat_vid, edges in self.next_pat_grouped_edges.items():
                next_vid_grouped_connective_edges: dict[DgVid, list[DataEdge]] = {}

                for edge in edges:
                    # 挑选出 `可连接的` 的边
                    if connective_e_vid := dg.get_first_connective_vid_of_e(edge):
                        dangling_e_vid = (
                            edge.dst_vid
                            if connective_e_vid == edge.src_vid
                            else edge.src_vid
                        )
                        # 挑选 `可连接到下一个模式点` 的边
                        if does_data_v_satisfy_pattern(
                            dangling_e_vid, next_pat_vid, pattern_vs, storage_adapter
                        ):
                            next_vid_grouped_connective_edges.setdefault(
                                dangling_e_vid, []
                            ).append(edge)
                            is_curr_dg_expandable = True

                # 如果当前匹配 `不可扩张`, 直接跳过
                if not is_curr_dg_expandable:
                    continue

                # 指定位置, 构造 `扩展图`
                # 注意! 对每一个 `下一个数据点`, 都要各自构造一个 `扩展图`
                for connective_edges in next_vid_grouped_connective_edges.values():
                    expanding_dg = ExpandGraph(deepcopy(dg))
                    expanding_dg.update_available_dangling_edges(connective_edges)
                    self.next_pat_grouped_expanding.setdefault(next_pat_vid, []).append(
                        expanding_dg
                    )

        self.all_matched.clear()
        self.next_pat_grouped_edges.clear()


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
        curr_group = A_bucket.next_pat_grouped_expanding.pop(curr_pat_vid, [])
        if not curr_group:
            return cls()

        all_expanded: list[ExpandGraph] = []

        for expanding in curr_group:
            # 与给定点集 `loaded_vertices` 求交
            expanding.update_available_target_vertices(loaded_vertices)

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
            expanding.update_available_target_vertices(loaded_vertices)

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
        left_group = left.next_pat_grouped_expanding.pop(target_pat_vid, [])
        right_group = right.next_pat_grouped_expanding.pop(target_pat_vid, [])
        expanding_graphs = cls.expand_edges_on_same_vertices(left_group, right_group)
        return cls(target_pat_vid, expanding_graphs)

    @classmethod
    def build_from_T_A(cls, left: "T_Bucket", right: A_Bucket):
        left_group = left.expanding_graphs
        right_group = right.next_pat_grouped_expanding.pop(left.target_pat_vid, [])

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
                    # 没有共同点, 属于另一种算法
                    unions = ExpandGraph.union_then_intersect_on_connective_v(
                        outer, inner
                    )
                    result.extend(unions)
                    continue

                # 先在 `点` 上取交集

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
