from dataclasses import dataclass, field
from enum import Enum

from schema import Edge, Eid, Vertex, Vid


class VNode:
    """
    点节点

    - 无垂悬邻接边
    """

    ev_in: dict[Eid, Vid] = field(default_factory=dict)
    """ { 入边 eid -> 入边起点 vid } """
    ev_out: dict[Eid, Vid] = field(default_factory=dict)
    """ { 出边 eid -> 出边终点 vid } """


@dataclass
class DanglingVNode:
    """
    点节点

    - 邻接边可以垂悬
    """

    e_in: set[Eid] = field(default_factory=set)
    """ 入边集 { eid } """
    e_out: set[Eid] = field(default_factory=set)
    """ 出边集 { eid } """


class RemoveVCascadeLevel(Enum):
    """
    移除点-级联级别

    0. 仅删除自身点
    1. 删除自身点及关联边
    2. 删除自身点、关联边及关联点
    """

    SelfOnly = 0
    """仅删除自身点"""

    WithEdges = 1
    """删除自身点及关联边"""

    WithEdgesAndVs = 2
    """删除自身点、关联边及关联点"""


@dataclass
class DynGraph:
    """
    动态多重有向图 (不允许 `垂悬边`)

    - 类似 networkx.MultiDiGraph
    """

    v_entities: dict[Vid, Vertex] = field(default_factory=dict)
    """ { vid -> Vertex } """
    e_entities: dict[Eid, Edge] = field(default_factory=dict)
    """ { eid -> Edge } """

    adj_table: dict[Vid, VNode] = field(default_factory=dict)
    """ 邻接表 { vid -> VNode } """
    dangling_adj_table: dict[Vid, DanglingVNode] = field(default_factory=dict)
    """ 垂悬邻接表 { vid -> DanglingVNode } """

    """ ========== 基本操作 ========== """

    def has_vid(self, vid: Vid):
        return vid in self.v_entities

    def has_all_vids(self, vids: list[Vid]):
        return all(vid in self.v_entities for vid in vids)

    def has_eid(self, eid: Eid):
        return eid in self.e_entities

    def has_all_eids(self, eids: list[Eid]):
        return all(eid in self.e_entities for eid in eids)

    def get_v_from_vid(self, vid: Vid):
        return self.v_entities.get(vid)

    def get_e_from_eid(self, eid: Eid):
        return self.e_entities.get(eid)

    """ ========== 底层功能 ========== """

    def update_v(self, vertex: Vertex):
        """更新点信息"""

        self.v_entities[vertex.vid] = vertex

    def update_v_batch(self, vertices: list[Vertex]):
        """批量更新点信息"""

        self.v_entities.update({v.vid: v for v in vertices})

    def update_e(self, edge: Edge):
        """更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        self.e_entities[edge.eid] = edge

        if self.has_all_vids([edge.src_vid, edge.dst_vid]):
            # src_v -[edge]-> dst_v
            src_v = self.adj_table.setdefault(edge.src_vid, VNode())
            dst_v = self.adj_table.setdefault(edge.dst_vid, VNode())
            src_v.ev_out[edge.eid] = edge.dst_vid
            dst_v.ev_in[edge.eid] = edge.src_vid

        elif self.has_vid(edge.src_vid):
            # src_v -[edge]-> ?
            src_v = self.dangling_adj_table.setdefault(edge.src_vid, DanglingVNode())
            src_v.e_out.add(edge.eid)

        elif self.has_vid(edge.dst_vid):
            # ? -[edge]-> dst_v
            dst_v = self.dangling_adj_table.setdefault(edge.dst_vid, DanglingVNode())
            dst_v.e_in.add(edge.eid)

    def update_e_batch(self, edges: list[Edge]):
        """批量更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        for edge in edges:
            self.update_e(edge)

    def remove_e(self, eid: Eid):
        """删除边信息"""
        if not self.has_eid(eid):
            return

        self.e_entities.pop(eid, None)

        for v in self.adj_table.values():
            v.ev_in.pop(eid, None)
            v.ev_out.pop(eid, None)

        for v in self.dangling_adj_table.values():
            v.e_in.discard(eid)
            v.e_out.discard(eid)

    def remove_e_batch(self, eids: list[Eid]):
        """批量删除边信息"""
        for eid in eids:
            self.remove_e(eid)

    def remove_v(
        self, vid: Vid, level: RemoveVCascadeLevel = RemoveVCascadeLevel.SelfOnly
    ):
        """删除点信息 (可指定级联级别)"""

        if not self.has_vid(vid):
            return

        self.v_entities.pop(vid)
        v_node = self.adj_table.pop(vid)

        for v in self.adj_table.values():
            v.ev_in = {e: v for e, v in v.ev_in.items() if v != vid}
            v.ev_out = {e: v for e, v in v.ev_out.items() if v != vid}

        if level == RemoveVCascadeLevel.WithEdges:
            # 删除关联边

            for e, v in v_node.ev_in.items():
                self.remove_e(e)

            for e, v in v_node.ev_out.items():
                self.remove_e(e)

    def remove_v_batch_same_level(
        self,
        vids: list[Vid],
        level: RemoveVCascadeLevel = RemoveVCascadeLevel.SelfOnly,
    ):
        """批量删除点信息 (可指定级联级别)"""

        for vid in vids:
            self.remove_v(vid, level)

    def remove_v_batch_diff_level(
        self,
        vids: list[Vid],
        levels: list[RemoveVCascadeLevel],
    ):
        """批量删除点信息 (可指定不同级联级别)"""

        # len(vids) <= len(levels) 时, 以 vids 为准
        for vid, level in zip(vids, levels):
            self.remove_v(vid, level)

        # 否则广播, 没匹配到的 vid 统统用 RemoveVCascadeLevel.SelfOnly
        if len(vids) > len(levels):
            for vid in vids[len(levels) :]:
                self.remove_v(vid, RemoveVCascadeLevel.SelfOnly)

    """ ========== 高级功能 ========== """

    def get_all_e_between(
        self, src_vid: Vid, dst_vid: Vid, bidirectional: bool = False
    ):
        """获取两点间的所有边 (可以指定是否双向)"""

        eids = set[Eid]()

        src_v = self.adj_table.get(src_vid)
        dst_v = self.adj_table.get(dst_vid)

        if src_v and dst_v:
            for e, v in src_v.ev_out.items():
                if v == dst_vid:
                    eids.add(e)

            if bidirectional:
                for e, v in dst_v.ev_out.items():
                    if v == src_vid:
                        eids.add(e)

        return set(self.get_e_from_eid(eid) for eid in eids)
