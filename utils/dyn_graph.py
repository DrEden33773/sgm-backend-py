from dataclasses import dataclass, field
from typing import Any

from schema import DataEdge, DataVertex, EdgeBase, Eid, VertexBase, Vid

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

    - 邻接边 `不可以垂悬`
    """

    e_in: set[Eid] = field(default_factory=set)
    """ 入边集 { eid } """
    e_out: set[Eid] = field(default_factory=set)
    """ 出边集 { eid } """

    def __or__(self, other: "VNode"):
        """取并集"""
        return VNode(
            e_in=self.e_in | other.e_in,
            e_out=self.e_out | other.e_out,
        )

    def __ior__(self, other: "VNode"):
        """原位生效: 取并集"""
        self.e_in |= other.e_in
        self.e_out |= other.e_out
        return self

    def __hash__(self) -> int:
        """VNode 的哈希值"""
        return hash(tuple(sorted(self.e_in | self.e_out)))


@dataclass
class DynGraph[VType: VertexBase = DataVertex, EType: EdgeBase = DataEdge]:
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

    v_2_pattern: dict[Vid, str] = field(default_factory=dict)
    """ 
    点模式映射
    - { vid -> pattern_str }
    """

    pattern_2_vs: dict[str, set[Vid]] = field(default_factory=dict)
    """ 
    模式-点映射
    - { pattern_str -> [vid] }
    """

    e_2_pattern: dict[Eid, str] = field(default_factory=dict)
    """
    边模式映射
    - { eid -> pattern_str }
    """

    pattern_2_es: dict[str, set[Eid]] = field(default_factory=dict)
    """ 
    模式-边映射
    - { pattern_str -> [eid] }
    """

    adj_table: dict[Vid, VNode] = field(default_factory=dict)
    """ 
    邻接表 
    - { vid -> VNode }
    """

    def __hash__(self):
        """图的哈希值"""
        return hash(tuple(sorted(self.adj_table.items())))

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

    def is_e_connective(self, edge: EType):
        """边是否具有连接性 (至少一个顶点存在, 因为允许 `半垂悬边`)"""
        return self.has_any_vid([edge.src_vid, edge.dst_vid])

    def is_e_full_connective(self, edge: EType):
        """边是否具有连接性 (两个顶点都存在)"""
        return self.has_all_vids([edge.src_vid, edge.dst_vid])

    def get_first_connective_vid_of_e(self, edge: EType):
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

    def get_v_pat_str_set(self):
        """获取所有点的模式字符串集合"""
        return set(self.v_2_pattern.values())

    def get_e_pat_str_set(self):
        """获取所有边的模式字符串集合"""
        return set(self.e_2_pattern.values())

    def get_all_pat_str_set(self):
        """获取所有模式字符串集合"""
        return self.get_v_pat_str_set() | self.get_e_pat_str_set()

    """ ========== 基本操作 ========== """

    def update_v(self, vertex: VType, pat_str: str):
        """更新点信息"""

        self.v_entities[vertex.vid] = vertex
        self.adj_table.setdefault(vertex.vid, VNode())

        old_pattern = self.v_2_pattern.get(vertex.vid, None)
        self.v_2_pattern[vertex.vid] = pat_str
        if old_pattern:
            self.pattern_2_vs[old_pattern].discard(vertex.vid)
        self.pattern_2_vs.setdefault(pat_str, set()).add(vertex.vid)

        return self

    def update_v_batch(self, vertices: list[VType], pat_strs: list[str]):
        """批量更新点信息"""

        assert len(vertices) == len(pat_strs)
        for vertex, pat_str in zip(vertices, pat_strs):
            self.update_v(vertex, pat_str)

        return self

    def update_e(self, edge: EType, pat_str: str):
        """更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        if self.has_all_vids([edge.src_vid, edge.dst_vid]):
            self.e_entities[edge.eid] = edge

            # src_v -[edge]-> dst_v
            src_v = self.adj_table.setdefault(edge.src_vid, VNode())
            dst_v = self.adj_table.setdefault(edge.dst_vid, VNode())
            src_v.e_out.add(edge.eid)
            dst_v.e_in.add(edge.eid)

            old_pattern = self.e_2_pattern.get(edge.eid, None)
            self.e_2_pattern[edge.eid] = pat_str
            if old_pattern:
                self.pattern_2_es[old_pattern].discard(edge.eid)
            self.pattern_2_es.setdefault(pat_str, set()).add(edge.eid)

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

    def update_e_batch(self, edges: list[EType], pat_strs: list[str]):
        """批量更新边信息 (`顶点不全存在` 的边, 视为垂悬)"""

        assert len(edges) == len(pat_strs)
        for edge, pat_str in zip(edges, pat_strs):
            self.update_e(edge, pat_str)

        return self

    def remove_e(self, eid: Eid, has_handled_adj_table: bool = False):
        """删除边信息"""
        if not self.has_eid(eid):
            return

        if not has_handled_adj_table:
            # 摘除边
            for v in self.adj_table.values():
                v.e_in.discard(eid)
                v.e_out.discard(eid)

        # 删除实体映射
        self.e_entities.pop(eid, None)

        old_pattern = self.e_2_pattern.pop(eid, None)
        if old_pattern:
            self.pattern_2_es[old_pattern].discard(eid)

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

    def __le__(self, other: "DynGraph[VType, EType]"):
        """self 是否为 other 的子图"""

        # 检验 `邻接表`
        for vid, v_node in self.adj_table.items():
            # `顶点`
            if vid not in other.adj_table:
                return False
            # `入边`
            if not v_node.e_in <= other.adj_table[vid].e_in:
                return False
            # `出边`
            if not v_node.e_out <= other.adj_table[vid].e_out:
                return False

        # 检验 `点模式映射`
        for vid, v_pat in self.v_2_pattern.items():
            if vid not in other.v_2_pattern:
                return False
            if v_pat != other.v_2_pattern[vid]:
                return False

        # 检验 `边模式映射`
        for eid, e_pat in self.e_2_pattern.items():
            if eid not in other.e_2_pattern:
                return False
            if e_pat != other.e_2_pattern[eid]:
                return False

        return True

    def __ge__(self, other: "DynGraph[VType, EType]"):
        """self 是否为 other 的超图"""

        return other.__le__(self)

    def __lt__(self, other: "DynGraph[VType, EType]"):
        """self 是否为 other 的真子图"""

        for vid, v_node in self.adj_table.items():
            if vid not in other.adj_table:
                return False
            if not v_node.e_in < other.adj_table[vid].e_in:
                return False
            if not v_node.e_out < other.adj_table[vid].e_out:
                return False

        for vid, v_pat in self.v_2_pattern.items():
            if vid not in other.v_2_pattern:
                return False
            if v_pat != other.v_2_pattern[vid]:
                return False

        for eid, e_pat in self.e_2_pattern.items():
            if eid not in other.e_2_pattern:
                return False
            if e_pat != other.e_2_pattern[eid]:
                return False

        return True

    def __gt__(self, other: "DynGraph[VType, EType]"):
        """self 是否为 other 的真超图"""

        return other.__lt__(self)

    def __eq__(self, other: Any):
        if not isinstance(other, DynGraph):
            return False
        else:
            return (
                self.adj_table == other.adj_table
                and self.e_2_pattern == other.e_2_pattern
                and self.v_2_pattern == other.v_2_pattern
            )

    def __ne__(self, other: Any):
        return not self.__eq__(other)

    def __or__(self, other: "DynGraph[VType, EType]"):
        """图与图的并集 (union-with-intersection)"""

        res = DynGraph(
            v_entities={**self.v_entities, **other.v_entities},
            e_entities={**self.e_entities, **other.e_entities},
            v_2_pattern={**self.v_2_pattern, **other.v_2_pattern},
            e_2_pattern={**self.e_2_pattern, **other.e_2_pattern},
            # pattern_2_vs={**self.pattern_2_vs, **other.pattern_2_vs}, # BUG
            # pattern_2_es={**self.pattern_2_es, **other.pattern_2_es}, # BUG
        )

        # 单独处理 邻接表
        for vid, v_node in self.adj_table.items():
            res.adj_table[vid] = v_node
        for vid, v_node in other.adj_table.items():
            if vid not in res.adj_table:
                res.adj_table[vid] = v_node
            else:
                # 取并集
                res.adj_table[vid] |= v_node

        # 正确合并 pattern_2_vs
        for pat, vs in self.pattern_2_vs.items():
            res.pattern_2_vs.setdefault(pat, set()).update(vs)
        for pat, vs in other.pattern_2_vs.items():
            res.pattern_2_vs.setdefault(pat, set()).update(vs)

        # 正确合并 pattern_2_es
        for pat, es in self.pattern_2_es.items():
            res.pattern_2_es.setdefault(pat, set()).update(es)
        for pat, es in other.pattern_2_es.items():
            res.pattern_2_es.setdefault(pat, set()).update(es)

        return res
