from copy import deepcopy
from dataclasses import dataclass, field

from config import DIRECTED_EDGE_SUPPORT
from executor.matching_ctx.type_aliases import DgVid, PgVid
from schema import DataEdge, DataVertex, PatternEdge, PatternVertex
from schema.basic import str_op_to_operator
from storage.abc import StorageAdapter
from utils.dyn_graph import DynGraph
from utils.expanding_graph import ExpandGraph


def does_data_v_satisfy_pattern(
    dg_vid: DgVid,
    pat_vid: PgVid,
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
    elif not data_v.attrs:
        # 模式点规定了属性, 但是数据点没有属性
        return False
    else:
        operator = str_op_to_operator(pattern_v.attr.op)
        if pattern_v.attr.key not in data_v.attrs:
            # 模式点规定了属性, 但是数据点没有这个属性
            return False
        data_value = data_v.attrs[pattern_v.attr.key]
        if type(data_value) is not type(pattern_v.attr.value):
            # 模式点规定了属性, 但是数据点的属性类型不匹配
            return False
        return operator(data_value, pattern_v.attr.value)


@dataclass
class f_Bucket:
    """枚举目标 (f) 桶"""

    all_matched: list[DynGraph] = field(default_factory=list)
    matched_with_pivots: dict[int, list[DgVid]] = field(default_factory=dict)

    @classmethod
    def from_C_bucket(cls, C_bucket: "C_Bucket"):
        all_matched: list[DynGraph] = []

        # 现在的算法, 会在 C_bucket 阶段, 直接完成基于 `下一个数据点` 的 `分裂`
        all_matched = [g.to_dyn_graph_cloned() for g in C_bucket.all_expanded]
        matched_with_pivots = {
            idx: C_bucket.expanded_idx_with_pivots[idx]
            for idx, _ in enumerate(C_bucket.all_expanded)
        }

        return cls(all_matched, matched_with_pivots)


@dataclass
class A_Bucket:
    """邻接组合 (A) 桶"""

    curr_pat_vid: PgVid
    all_matched: list[DynGraph] = field(default_factory=list)
    matched_with_pivots: dict[int, list[DgVid]] = field(default_factory=dict)

    next_pat_grouped_expanding: dict[PgVid, list[ExpandGraph]] = field(
        default_factory=dict
    )

    @classmethod
    def from_f_bucket(cls, curr_pat_vid: PgVid, f_bucket: f_Bucket):
        return cls(
            curr_pat_vid,
            f_bucket.all_matched,
            f_bucket.matched_with_pivots,
        )

    def incremental_load_new_edges(
        self,
        pattern_es: list[PatternEdge],
        pattern_vs: dict[PgVid, PatternVertex],
        storage_adapter: StorageAdapter,
    ):
        connected_data_vids: set[DgVid] = set()

        # 迭代 `已匹配` 的数据图
        for idx, pivot_vids in self.matched_with_pivots.items():
            matched_dg = self.all_matched[idx]

            # 迭代 `边缘点` (当前 `数据图` 上)
            for pivot_vid in pivot_vids:
                is_pivot_vid_connected = False

                # 迭代 `模式边`
                for pat_e in pattern_es:
                    # 如果 `matched_dg` 中已经包含 `pat_e` 这条模式边, 应该跳过
                    if pat_e.eid in matched_dg.get_e_pat_str_set():
                        continue

                    label, attr = pat_e.label, pat_e.attr
                    next_vid_grouped_conn_es: dict[DgVid, list[DataEdge]] = {}
                    next_vid_grouped_conn_pat_strs: dict[DgVid, list[str]] = {}

                    if self.curr_pat_vid == pat_e.src_vid:
                        next_pat_vid = pat_e.dst_vid
                        # 挑选 `可连接到下一个模式点` 的边
                        matched_data_es = [
                            e
                            for e in (
                                storage_adapter.load_e_by_src_vid(pivot_vid, label)
                                if not attr
                                else storage_adapter.load_e_by_src_vid_with_attr(
                                    pivot_vid, label, attr
                                )
                            )
                            if does_data_v_satisfy_pattern(
                                e.dst_vid,
                                next_pat_vid,
                                pattern_vs,
                                storage_adapter,
                            )
                            and e.eid not in matched_dg.e_entities
                        ]
                        if not DIRECTED_EDGE_SUPPORT:
                            # 如果不支持有向边, 就该把 `反方向` 的边也加载进来
                            # 挑选 `可连接到下一个模式点` 的边
                            matched_data_es += [
                                e
                                for e in (
                                    storage_adapter.load_e_by_dst_vid(pivot_vid, label)
                                    if not attr
                                    else storage_adapter.load_e_by_dst_vid_with_attr(
                                        pivot_vid, label, attr
                                    )
                                )
                                if does_data_v_satisfy_pattern(
                                    e.src_vid,
                                    next_pat_vid,
                                    pattern_vs,
                                    storage_adapter,
                                )
                                and e.eid not in matched_dg.e_entities
                            ]
                        # 按照 `下一个数据点` 分组
                        for e in matched_data_es:
                            next_vid_grouped_conn_es.setdefault(e.dst_vid, []).append(e)
                            next_vid_grouped_conn_pat_strs.setdefault(
                                e.dst_vid, []
                            ).append(pat_e.eid)
                    else:
                        next_pat_vid = pat_e.src_vid
                        # 挑选 `可连接到下一个模式点` 的边
                        matched_data_es = [
                            e
                            for e in (
                                storage_adapter.load_e_by_dst_vid(pivot_vid, label)
                                if not attr
                                else storage_adapter.load_e_by_dst_vid_with_attr(
                                    pivot_vid, label, attr
                                )
                            )
                            if does_data_v_satisfy_pattern(
                                e.src_vid,
                                next_pat_vid,
                                pattern_vs,
                                storage_adapter,
                            )
                            and e.eid not in matched_dg.e_entities
                        ]
                        if not DIRECTED_EDGE_SUPPORT:
                            # 如果不支持有向边, 就该把 `反方向` 的边也加载进来
                            # 挑选 `可连接到下一个模式点` 的边
                            matched_data_es += [
                                e
                                for e in (
                                    storage_adapter.load_e_by_src_vid(pivot_vid, label)
                                    if not attr
                                    else storage_adapter.load_e_by_src_vid_with_attr(
                                        pivot_vid, label, attr
                                    )
                                )
                                if does_data_v_satisfy_pattern(
                                    e.dst_vid,
                                    next_pat_vid,
                                    pattern_vs,
                                    storage_adapter,
                                )
                                and e.eid not in matched_dg.e_entities
                            ]
                        # 按照 `下一个数据点` 分组
                        for e in matched_data_es:
                            next_vid_grouped_conn_es.setdefault(e.src_vid, []).append(e)
                            next_vid_grouped_conn_pat_strs.setdefault(
                                e.src_vid, []
                            ).append(pat_e.eid)

                    if not matched_data_es:
                        continue

                    is_pivot_vid_connected = True

                    # 开始构造 `扩张图`
                    # 注意! 对每一个 `下一个数据点`, 都要各自构造一个 `扩张图`
                    for key, edges in next_vid_grouped_conn_es.items():
                        expanding_dg = ExpandGraph(deepcopy(matched_dg))
                        pat_strs = next_vid_grouped_conn_pat_strs[key]
                        expanding_dg.update_valid_dangling_edges(edges, pat_strs)
                        self.next_pat_grouped_expanding.setdefault(
                            next_pat_vid, []
                        ).append(expanding_dg)

                # 如果当前 `数据图` 上的 `边缘点` 连接了 `模式边`, 那么就要更新 `已连接点集`
                if is_pivot_vid_connected:
                    connected_data_vids.add(pivot_vid)

        self.all_matched.clear()

        return connected_data_vids


@dataclass
class C_Bucket:
    """候选集 (C) 桶"""

    all_expanded: list[ExpandGraph] = field(default_factory=list)
    expanded_idx_with_pivots: dict[int, list[DgVid]] = field(default_factory=dict)

    @classmethod
    def build_from_A(
        cls,
        A_bucket: A_Bucket,
        curr_pat_vid: PgVid,
        loaded_vs: list[DataVertex],
        loaded_v_pat_strs: list[str],
    ):
        # 从 A_bucket 中弹出当前分组
        curr_group = A_bucket.next_pat_grouped_expanding.pop(curr_pat_vid, [])
        if not curr_group:
            return cls()

        all_expanded: list[ExpandGraph] = []
        expanded_with_pivots: dict[int, list[DgVid]] = {}

        for idx, expanding in enumerate(curr_group):
            # 与给定点集 `loaded_vertices` 求交
            valid_targets = expanding.update_valid_target_vertices(
                loaded_vs, loaded_v_pat_strs
            )

            # 更新 `all_expanded`
            all_expanded.append(expanding)

            # 更新 `expanded_with_pivots`
            expanded_with_pivots.setdefault(idx, []).extend(
                [target.vid for target in valid_targets]
            )

        # 从 `A_bucket` 构造的图, 后续还需要 `进一步枚举`
        return cls(all_expanded, expanded_with_pivots)

    @classmethod
    def build_from_T(
        cls,
        T_bucket: "T_Bucket",
        loaded_vertices: list[DataVertex],
        loaded_v_pat_strs: list[str],
    ):
        if not T_bucket.expanding_graphs:
            return cls()

        all_expanded: list[ExpandGraph] = []
        expanded_with_pivots: dict[int, list[DgVid]] = {}

        for idx, expanding in enumerate(T_bucket.expanding_graphs):
            # 与给定点集 `loaded_vertices` 求交
            valid_targets = expanding.update_valid_target_vertices(
                loaded_vertices, loaded_v_pat_strs
            )

            # 更新 `all_expanded`
            all_expanded.append(expanding)

            # 更新 `expanded_with_pivots`
            expanded_with_pivots.setdefault(idx, []).extend(
                [target.vid for target in valid_targets]
            )

        # 从 `T_bucket` 构造的图, 已经被 `完全枚举`
        return cls(all_expanded, expanded_with_pivots)


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
        expanding_graphs = cls.expand_edges_of_two(left_group, right_group)
        return cls(target_pat_vid, expanding_graphs)

    @classmethod
    def build_from_T_A(cls, left: "T_Bucket", right: A_Bucket):
        left_group = left.expanding_graphs
        right_group = right.next_pat_grouped_expanding.pop(left.target_pat_vid, [])
        expanding_graphs = cls.expand_edges_of_two(left_group, right_group)
        return cls(left.target_pat_vid, expanding_graphs)

    @staticmethod
    def expand_edges_of_two(
        left_group: list[ExpandGraph],
        right_group: list[ExpandGraph],
    ):
        """分 `有公共点` 和 `无公共点`, 将两张图 `枚举式` 的拓展成新图"""

        result: list[ExpandGraph] = []

        outer_, inner_ = left_group, right_group
        if len(outer_) > len(inner_):
            outer_, inner_ = inner_, outer_

        for outer in outer_:
            for inner in inner_:
                unions = ExpandGraph.union_then_intersect_on_connective_v(outer, inner)
                result.extend(unions)

        return result
