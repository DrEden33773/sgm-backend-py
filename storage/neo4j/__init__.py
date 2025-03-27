from functools import lru_cache
from typing import Any, override

from neo4j import GraphDatabase, Query
from schema import DataEdge, DataVertex, Label, PatternAttr, Vid
from storage.abc import StorageAdapter
from utils.tracked_lru_cache import track_lru_cache_annotated


class Neo4jStorageAdapter(StorageAdapter):
    """Neo4j 存储适配器"""

    def __init__(
        self,
        url: str = "bolt://localhost:7687",
        username: str = "neo4j",
        password: str = "password",
    ) -> None:
        super().__init__()
        self.driver = GraphDatabase.driver(url, auth=(username, password))

    def execute_query(
        self, query: Query, parameters: dict[str, Any] = {}
    ) -> list[dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record.data() for record in result]

    def node_to_vertex(self, v_label: str, received: dict[str, Any]) -> DataVertex:
        vid = str(received.pop("id"))
        attrs: dict[str, int | float | str] = {}
        for name, val in received.items():
            typed_val = str(val)
            if type(val) is int:
                typed_val = int(val)
            elif type(val) is float:
                typed_val = float(val)
            attrs[name] = typed_val
        return DataVertex(vid=vid, label=v_label, attrs=attrs)

    def relationship_to_edge(self, e_label: str, received: dict[str, Any]) -> DataEdge:
        eid = "<USELESS>"
        src_vid = str(received.pop("src_vid"))
        dst_vid = str(received.pop("dst_vid"))
        attrs: dict[str, int | float | str] = {}
        for name, val in received.items():
            typed_val = str(val)
            if type(val) is int:
                typed_val = int(val)
            elif type(val) is float:
                typed_val = float(val)
            attrs[name] = typed_val
        return DataEdge(
            eid=eid, label=e_label, src_vid=src_vid, dst_vid=dst_vid, attrs=attrs
        )

    @override
    @track_lru_cache_annotated
    @lru_cache
    def get_v(self, vid: str) -> DataVertex:
        query = Query("MATCH (v) WHERE v.id = $vid RETURN v, labels(v) AS v_label")
        results = self.execute_query(query, {"vid": vid})
        if not results:
            raise RuntimeError(f"DataVertex (vid: {vid}) not found.")

        received = results[0].pop("v")
        labels: list[str] = received.pop("v_label")
        return self.node_to_vertex(labels[0], received)

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        query = Query("MATCH (v:$v_label) RETURN v")
        results = self.execute_query(query, {"v_label": v_label})
        if not results:
            return []

        vertices: list[DataVertex] = []
        for result in results:
            received = result.pop("v")
            vertices.append(self.node_to_vertex(v_label, received))

        return vertices

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        query = Query("MATCH (v:$v_label) WHERE $attr RETURN v")
        results = self.execute_query(
            query, {"v_label": v_label, "attr": v_attr.to_neo4j_where_sub_sentence()}
        )
        if not results:
            return []

        vertices: list[DataVertex] = []
        for result in results:
            received = result.pop("v")
            vertices.append(self.node_to_vertex(v_label, received))

        return vertices

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid(self, src_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = Query(
            """ 
            MATCH (src)-[e:$e_label]->(dst) 
            WHERE src.id = $src_vid 
            RETURN e, src.id AS src_vid, dst.id AS dst_vid
            """
        )
        results = self.execute_query(query, {"src_vid": src_vid, "e_label": e_label})
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            received = result.pop("e")
            edges.append(self.relationship_to_edge(e_label, received))
        return edges

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid(self, dst_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = Query(
            """ 
            MATCH (src)-[e:$e_label]->(dst) 
            WHERE dst.id = $dst_vid 
            RETURN e, src.id AS src_vid, dst.id AS dst_vid
            """
        )
        results = self.execute_query(query, {"dst_vid": dst_vid, "e_label": e_label})
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            received = result.pop("e")
            edges.append(self.relationship_to_edge(e_label, received))
        return edges

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid_with_attr(
        self,
        src_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = Query(
            """ 
            MATCH (src)-[e:$e_label]->(dst) 
            WHERE src.id = $src_vid
            AND $attr 
            RETURN e, src.id AS src_vid, dst.id AS dst_vid
            """
        )
        results = self.execute_query(
            query,
            {
                "src_vid": src_vid,
                "e_label": e_label,
                "attr": e_attr.to_neo4j_where_sub_sentence(),
            },
        )
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            received = result.pop("e")
            edges.append(self.relationship_to_edge(e_label, received))
        return edges

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid_with_attr(
        self,
        dst_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = Query(
            """ 
            MATCH (src)-[e:$e_label]->(dst) 
            WHERE dst.id = $dst_vid
            AND $attr 
            RETURN e, src.id AS src_vid, dst.id AS dst_vid
            """
        )
        results = self.execute_query(
            query,
            {
                "dst_vid": dst_vid,
                "e_label": e_label,
                "attr": e_attr.to_neo4j_where_sub_sentence(),
            },
        )
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            received = result.pop("e")
            edges.append(self.relationship_to_edge(e_label, received))
        return edges
