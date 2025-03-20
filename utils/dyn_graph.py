from dataclasses import dataclass, field
from enum import Enum

from schema import DataEdge, DataVertex, Eid, VertexBase, Vid

COMPLETELY_DANGLE_BASE = """
Detected `completely-dangling edge`:
    ? -[eid: {}]-> ? \
"""

DANGLE_ON_SRC_BASE = """
Detected `half-dangling edge`:
    ? -[eid: {}]-> (vid: {}) \
"""

DANGLE_ON_DST_BASE = """
Detected `half-dangling edge`:
    (vid: {}) -[eid: {}]-> ? \
"""


@dataclass
class VNode:
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
class DynGraph[VType: VertexBase = DataVertex, EType: DataEdge = DataEdge]:
    """
    动态多重有向图 (不允许 `垂悬边`)

    - 类似 networkx.MultiDiGraph
    """

    v_entities: dict[Vid, VType] = field(default_factory=dict)
    """ 
    点实体 
    - { vid -> Vertex }
    """
    e_entities: dict[Eid, EType] = field(default_factory=dict)
    """ 
    边实体 
    - { eid -> Edge }
    """
    adj_table: dict[Vid, VNode] = field(default_factory=dict)
    """ 
    邻接表 
    - { vid -> VNode }
    """

    """ ========== 底层功能 ========== """

    def has_vid(self, vid: Vid):
        return vid in self.v_entities

    def has_all_vids(self, vids: list[Vid]):
        return all(vid in self.v_entities for vid in vids)

    def has_any_vid(self, vids: list[Vid]):
        return any(vid in self.v_entities for vid in vids)

    def has_eid(self, eid: Eid):
        return eid in self.e_entities

    def has_all_eids(self, eids: list[Eid]):
        return all(eid in self.e_entities for eid in eids)

    def has_any_eid(self, eids: list[Eid]):
        return any(eid in self.e_entities for eid in eids)

    def get_v_from_vid(self, vid: Vid):
        return self.v_entities.get(vid)

    def get_e_from_eid(self, eid: Eid):
        return self.e_entities.get(eid)

    def is_e_connective(self, edge: DataEdge):
        """边是否具有连接性 (至少一个顶点存在, 因为允许 `半垂悬边`)"""
        return self.has_any_vid([edge.src_vid, edge.dst_vid])

    def is_e_full_connective(self, edge: DataEdge):
        """边是否具有连接性 (两个顶点都存在)"""
        return self.has_all_vids([edge.src_vid, edge.dst_vid])

    def get_first_connective_vid_of_e(self, edge: DataEdge):
        """获取边上的第一个 `可连接点` 的 `vid`"""

        if self.has_vid(edge.src_vid):
            return edge.src_vid

        if self.has_vid(edge.dst_vid):
            return edge.dst_vid

        return None

    def get_vid_set(self, ignore: set[Vid] = set()):
        """获取点集合"""
        return set(self.v_entities.keys()) - ignore

    def get_v_count(self):
        """获取点数量"""
        return len(self.v_entities)

    def get_e_count(self):
        """获取边数量"""
        return len(self.e_entities)

    """ ========== 基本操作 ========== """

    def update_v(self, vertex: VType):
        """更新点信息"""

        self.v_entities[vertex.vid] = vertex
        self.adj_table.setdefault(vertex.vid, VNode())

        return self

    def update_v_batch(self, vertices: list[VType]):
        """批量更新点信息"""

        for vertex in vertices:
            self.update_v(vertex)

        return self

    def update_e(self, edge: EType):
        """更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        self.e_entities[edge.eid] = edge

        if self.has_all_vids([edge.src_vid, edge.dst_vid]):
            # src_v -[edge]-> dst_v
            src_v = self.adj_table.setdefault(edge.src_vid, VNode())
            dst_v = self.adj_table.setdefault(edge.dst_vid, VNode())
            src_v.e_out.add(edge.eid)
            dst_v.e_in.add(edge.eid)
            return self

        if self.has_vid(edge.src_vid):
            # src_v -[edge]-> ? (? = unjoined `dst_v``)
            raise RuntimeError(DANGLE_ON_DST_BASE.format(edge.src_vid, edge.eid))

        elif self.has_vid(edge.dst_vid):
            # ? -[edge]-> dst_v (? = unjoined `src_v`)
            raise RuntimeError(DANGLE_ON_SRC_BASE.format(edge.dst_vid, edge.eid))

        else:
            # ? -[edge]-> ?
            raise RuntimeError(COMPLETELY_DANGLE_BASE.format(edge.eid))

    def update_e_batch(self, edges: list[EType]):
        """批量更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        for edge in edges:
            self.update_e(edge)

        return self

    def remove_e(self, eid: Eid, has_handled_adj_table: bool = False):
        """删除边信息"""
        if not self.has_eid(eid):
            return

        if not has_handled_adj_table:
            # 摘除关联边
            for v in self.adj_table.values():
                v.e_in.discard(eid)
                v.e_out.discard(eid)

        # 删除实体映射
        self.e_entities.pop(eid, None)

        return self

    def remove_e_batch(self, eids: list[Eid]):
        """批量删除边信息"""
        for eid in eids:
            self.remove_e(eid)

        return self

    """ ========== 高级功能 ========== """

    def get_all_e_between(
        self, src_vid: Vid, dst_vid: Vid, bidirectional: bool = False
    ):
        """获取两点间的所有边 (可以指定是否双向)"""

        eids = set[Eid]()

        src_v = self.adj_table.get(src_vid)
        dst_v = self.adj_table.get(dst_vid)

        if src_v and dst_v:
            # src_v -[eid]-> dst_v
            eids.update(src_v.e_out & dst_v.e_in)

            if bidirectional:
                # src_v <-[eid]- dst_v
                eids.update(src_v.e_in & dst_v.e_out)

        return set(self.get_e_from_eid(eid) for eid in eids)
