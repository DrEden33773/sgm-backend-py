from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache

from executor.matching_ctx.type_aliases import DgVid
from schema import DataEdge, DataVertex, EdgeBase, Eid, VertexBase, Vid
from utils.dyn_graph import DynGraph, VNode
from utils.tracked_lru_cache import track_lru_cache_annotated


@dataclass
class ExpandGraph[VType: VertexBase = DataVertex, EType: EdgeBase = DataEdge]:
    """
    `扩张中` / `刚好扩张结束` 的图

    - 主要用于存储含有 `半垂悬边` 的图.
    - 事实上, 这个类可以存储 `无垂悬边` 的图.
        - 它必须是某个 `有半垂悬边` 的图, 连接数个 `顶点` 后形成的.
    """

    dyn_graph: DynGraph[VType, EType]
    """ 图 """

    target_v_adj_table: dict[Vid, VNode] = field(default_factory=dict)
    """ `扩张终点` 的 `邻接表` """

    dangling_e_entities: dict[Eid, EType] = field(default_factory=dict)
    """ 垂悬边实体 """

    target_v_entities: dict[Vid, VType] = field(default_factory=dict)
    """ 扩张终点实体 """

    def __post_init__(self):
        self.get_vid_set = self.dyn_graph.get_vid_set
        self.get_v_count = self.dyn_graph.get_v_count

    def group_dangling_e_by_pending_v(self):
        """按照 `未连接的点` 对 `半垂悬边` 进行分组"""

        dangling_e_grouped: dict[Vid, list[EType]] = {}

        for dangling_e in self.dangling_e_entities.values():
            if self.dyn_graph.has_vid(dangling_e.src_vid):
                dangling_e_grouped.setdefault(dangling_e.dst_vid, []).append(dangling_e)
            else:
                dangling_e_grouped.setdefault(dangling_e.src_vid, []).append(dangling_e)

        return dangling_e_grouped

    def update_valid_dangling_edges(self, dangling_edges: list[EType]):
        """
        更新 `合法半垂悬边`, 返回 `不合法半垂悬边`
        """

        @track_lru_cache_annotated
        @lru_cache
        def is_valid_edge(edge: EType):
            return self.dyn_graph.is_e_connective(
                edge
            ) and not self.dyn_graph.is_e_full_connective(edge)

        legal_edges: set[EType] = set()
        illegal_edges: set[EType] = set()
        for edge in dangling_edges:
            if is_valid_edge(edge):
                legal_edges.add(edge)
            else:
                illegal_edges.add(edge)

        self.dangling_e_entities.update({e.eid: e for e in legal_edges})

        # 千万不要更新 `dyn_graph` 中的 `边`
        # 因为这个函数的目的, 是 `保留` edges 连接的 `图模式`
        # 更新 `dyn_graph` 交给 `to_dyn_graph_cloned` 来做

        return illegal_edges

    def update_valid_target_vertices(self, target_vertices: list[VType]):
        """
        更新 `合法扩张终点`, 返回 `不合法扩张终点`

        - 只有在 `垂悬边` 中的点才会被添加到 `邻接表` 中
        """

        @track_lru_cache_annotated
        @lru_cache
        def is_valid_target(v: VType):
            for edge in self.dangling_e_entities.values():
                if edge.__contains__(v.vid):
                    return True
            return False

        legal_targets: set[VType] = set()
        illegal_vertices: set[VType] = set()
        for v in target_vertices:
            if is_valid_target(v):
                legal_targets.add(v)
            else:
                illegal_vertices.add(v)

        self.target_v_entities.update({v.vid: v for v in legal_targets})

        # 千万不要更新 `dyn_graph` 中的 `点`
        # 因为这个函数的目的, 是 `保留` targets 连接的 `图模式`
        # 更新 `dyn_graph` 交给 `to_dyn_graph_cloned` 来做

        for dangling_e in self.dangling_e_entities:
            e = self.dangling_e_entities[dangling_e]
            if e.src_vid in self.target_v_entities:
                self.target_v_adj_table.setdefault(e.src_vid, VNode()).e_out.add(e.eid)
            if e.dst_vid in self.target_v_entities:
                self.target_v_adj_table.setdefault(e.dst_vid, VNode()).e_in.add(e.eid)

        return illegal_vertices

    def to_dyn_graph_cloned(self):
        new_dyn_graph = deepcopy(self.dyn_graph)

        # 先加上 `target_v`
        new_dyn_graph.update_v_batch(list(self.target_v_entities.values()))

        # 更新边的时候, 只根据 `target_v_adj_table` 更新
        # 也就是说, 自动忽略那些 `仍然半垂悬` 的边
        for target_v in self.target_v_adj_table:
            dangling_eids = self.target_v_adj_table[target_v].e_out
            dangling_eids |= self.target_v_adj_table[target_v].e_in
            dangling_es = [self.dangling_e_entities[eid] for eid in dangling_eids]
            new_dyn_graph.update_e_batch(dangling_es)

        return new_dyn_graph

    @staticmethod
    def intersect_then_union_on_same_v(
        potential_unused: "ExpandGraph", potential_incomplete: "ExpandGraph"
    ) -> list["ExpandGraph"]:
        """
        先确认两张图是否有 `公共点`

        有的话, 就试图把 `预期可以连在一起` 的 `半垂悬边` 连接起来
        """

        result: list["ExpandGraph"] = []

        # 顶点集更少的, 就是 unused
        unused, incomplete = potential_unused, potential_incomplete
        unused_set, incomplete_set = unused.get_vid_set(), incomplete.get_vid_set()

        if not unused_set <= incomplete_set:
            if not unused_set >= incomplete_set:
                # 没有共同点, 采用另一个算法
                return ExpandGraph.union_then_intersect_on_connective_v(
                    potential_unused, potential_incomplete
                )
            # 交换位置
            unused, incomplete = incomplete, unused

        grouped_incomplete_dangling_es = incomplete.group_dangling_e_by_pending_v()
        grouped_unused_dangling_es = unused.group_dangling_e_by_pending_v()

        # 注意, dangling_es 与 `incomplete` 中的任一组 `dangling_es` 有 `预期的共同点` 时, 才考虑合并
        for pending_v, dangling_es in grouped_unused_dangling_es.items():
            for expected_vid in grouped_incomplete_dangling_es:
                if pending_v != expected_vid:
                    continue
                # 每次合并, 直接开一张新图
                new_expanding_dg = deepcopy(incomplete)
                new_expanding_dg.update_valid_dangling_edges(dangling_es)
                result.append(new_expanding_dg)

        return result

    @staticmethod
    def union_then_intersect_on_connective_v(
        left_expand_graph: "ExpandGraph", right_expand_graph: "ExpandGraph"
    ) -> list["ExpandGraph"]:
        """
        先将两个图的 `点` 和 `非垂悬边` 合成到一张新的图里面

        然后再遍历两图的 `半垂悬边`, 选出那些可以相互连接的边

        - 注意: 这个策略有可能会匹配出 `规模比模式图大` 的 `数据图`
            - 这个例子, 详见 `main.py` 中 `test_more_triangle_plan`
            - 最简单的方法是, `Report` 阶段进行严格的 `点数 + 边数` 过滤
        """

        left_dyn_graph = left_expand_graph.dyn_graph
        right_dyn_graph = right_expand_graph.dyn_graph

        if left_dyn_graph.get_vid_set() & right_dyn_graph.get_vid_set():
            # 两个图存在 `公共点`, 转移到另一个算法
            return ExpandGraph.intersect_then_union_on_same_v(
                left_expand_graph, right_expand_graph
            )

        left_vs = list(left_dyn_graph.v_entities.values())
        right_vs = list(right_dyn_graph.v_entities.values())
        left_es = list(left_dyn_graph.e_entities.values())
        right_es = list(right_dyn_graph.e_entities.values())

        new_dyn_graph = (
            DynGraph()
            .update_v_batch(left_vs + right_vs)
            .update_e_batch(left_es + right_es)
        )
        dst_v_grouped_results: dict[DgVid, list[ExpandGraph]] = {}

        grouped_l_dangling_es = left_expand_graph.group_dangling_e_by_pending_v()
        grouped_r_dangling_es = right_expand_graph.group_dangling_e_by_pending_v()

        # (按照 pending_vid 分组) 遍历两图的 `半垂悬边`, 选出那些可以相互连接的边
        for l_pending_vid, l_dangling_es in grouped_l_dangling_es.items():
            for r_pending_vid, r_dangling_es in grouped_r_dangling_es.items():
                if l_pending_vid == r_pending_vid:
                    new_expanding_dg = ExpandGraph(new_dyn_graph)
                    new_expanding_dg.update_valid_dangling_edges(
                        l_dangling_es + r_dangling_es
                    )
                    dst_v_grouped_results.setdefault(l_pending_vid, []).append(
                        new_expanding_dg
                    )

        flattened: list[ExpandGraph] = []
        for results in dst_v_grouped_results.values():
            flattened.extend(results)
        return flattened
