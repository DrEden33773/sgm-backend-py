from copy import deepcopy
from dataclasses import dataclass, field
from functools import lru_cache

from schema import DataEdge, DataVertex, Eid, Vid
from utils.dyn_graph import DynGraph, VNode


@dataclass
class ExpandGraph:
    """
    `扩张中` / `刚好扩张结束` 的图

    - 对 `有半垂悬边的` 图的封装
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

        dangling_es = [
            self.dyn_graph.e_entities[eid] for eid in self.dyn_graph.half_dangling_es
        ]

        # 在当前类中, 添加 `垂悬边`
        for e in dangling_es:
            self.dangling_e_entities[e.eid] = e

        # 保留原图中的 `垂悬边`, 这样方便快速从 `ExpandGraph` 转为等效的 `DynGraph`
        return

    def update_available_targets(self, target_vertices: list[DataVertex]):
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

        for dangling_e in self.dangling_e_entities:
            e = self.dangling_e_entities[dangling_e]
            if e.src_vid in self.target_v_entities:
                self.target_v_adj_table.setdefault(e.src_vid, VNode()).e_out.add(e.eid)
            if e.dst_vid in self.target_v_entities:
                self.target_v_adj_table.setdefault(e.dst_vid, VNode()).e_in.add(e.eid)

        return illegal_vertices

    def to_dyn_graph_emplace(self):
        if not self.target_v_entities:
            return self.dyn_graph

        # 这里只需要更新点, 因为 `垂悬边` 并没有从 `dyn_graph` 中摘除
        self.dyn_graph.update_v_batch(list(self.target_v_entities.values()))
        return self.dyn_graph

    def to_dyn_graph_cloned(self):
        if not self.target_v_entities:
            return deepcopy(self.dyn_graph)

        new_dyn_graph = deepcopy(self.dyn_graph)

        # 这里只需要更新点, 因为 `垂悬边` 并没有从 `dyn_graph` 中摘除
        new_dyn_graph.update_v_batch(list(self.target_v_entities.values()))
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
            new_dyn_graph = deepcopy(incomplete.dyn_graph)
            new_dyn_graph.update_e(dangling_e)
            new_extending_graph = ExpandGraph(new_dyn_graph)
            result.append(new_extending_graph)

        return result
