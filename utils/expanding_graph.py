from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache

from schema import DataEdge, DataVertex, Eid, Vid
from utils.dyn_graph import DynGraph, VNode


@dataclass
class ExpandGraph:
    """
    `扩张中` / `刚好扩张结束` 的图

    - 主要用于存储含有 `半垂悬边` 的图.
    - 事实上, 这个类可以存储 `无垂悬边` 的图.
        - 它必须是某个 `有半垂悬边` 的图, 连接数个 `顶点` 后形成的.
    """

    dyn_graph: DynGraph
    """ 图 """

    target_v_adj_table: dict[Vid, VNode] = field(default_factory=dict)
    """ `扩张终点` 的 `邻接表` """

    dangling_e_entities: dict[Eid, DataEdge] = field(default_factory=dict)
    """ 垂悬边实体 """

    target_v_entities: dict[Vid, DataVertex] = field(default_factory=dict)
    """ 扩张终点实体 """

    def __post_init__(self):
        self.get_vid_set = self.dyn_graph.get_vid_set
        self.get_v_count = self.dyn_graph.get_v_count

    def update_available_dangling_edges(self, dangling_edges: list[DataEdge]):
        """
        更新 `合法半垂悬边`, 返回 `不合法半垂悬边`
        """

        @lru_cache
        def is_valid_edge(edge: DataEdge):
            return self.dyn_graph.is_e_connective(
                edge
            ) and not self.dyn_graph.is_e_full_connective(edge)

        legal_edges: set[DataEdge] = set()
        illegal_edges: set[DataEdge] = set()
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

    def update_available_target_vertices(self, target_vertices: list[DataVertex]):
        """
        更新 `合法扩张终点`, 返回 `不合法扩张终点`

        - 只有在 `垂悬边` 中的点才会被添加到 `邻接表` 中
        """

        @lru_cache
        def is_valid_target(v: DataVertex):
            for edge in self.dangling_e_entities.values():
                if v.vid in (edge.src_vid, edge.dst_vid):
                    return True

        legal_targets: set[DataVertex] = set()
        illegal_vertices: set[DataVertex] = set()
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
    ):
        """
        注意, 这里会按照 `unused` 边的数量, 拷贝多份 `incomplete` 图
        每份 incomplete 图, 都会连接一条 unused 边
        """

        result: list["ExpandGraph"] = []

        # 顶点集更少的, 就是 unused
        unused, incomplete = potential_unused, potential_incomplete
        unused_set, incomplete_set = unused.get_vid_set(), incomplete.get_vid_set()

        if not unused_set <= incomplete_set:
            if not unused_set >= incomplete_set:
                # 没有共同点
                return result
            # 交换位置
            unused, incomplete = incomplete, unused

        for dangling_e in unused.dangling_e_entities.values():
            new_expanding_dg = deepcopy(incomplete)
            new_expanding_dg.update_available_dangling_edges([dangling_e])
            result.append(new_expanding_dg)

        return result
