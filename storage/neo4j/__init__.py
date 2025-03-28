from functools import lru_cache
from typing import Any, LiteralString, cast, override

from neo4j import GraphDatabase, Query
from schema import DataEdge, DataVertex, Label, PatternAttr, Vid
from storage.abc import StorageAdapter
from utils.tracked_lru_cache import track_lru_cache_annotated


class Neo4jStorageAdapter(StorageAdapter):
    """
    Neo4j 存储适配器

    - 注意:
        - 凡是涉及到 eid / vid 这样的 `全局 ID`, 一概使用 `elementId(e/v)` 来查询
            - v.id 这个就不保险, 因为这是 `属性 ID`, 不保证全局作用域下的唯一性
    """

    def __init__(
        self,
        url: str = "bolt://localhost:7687",
        username: str = "neo4j",
        password: str = "12345678",
    ) -> None:
        super().__init__()
        self.driver = GraphDatabase.driver(url, auth=(username, password))

    def execute_query(self, query: Query | LiteralString) -> list[dict[str, Any]]:
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def node_to_vertex(
        self, vid: str, v_label: str, props: dict[str, Any]
    ) -> DataVertex:
        attrs: dict[str, int | float | str] = {}
        for name, val in props.items():
            typed_val = str(val)
            if isinstance(val, int):
                typed_val = int(val)
            elif isinstance(val, float):
                typed_val = float(val)
            attrs[name] = typed_val

        return DataVertex(vid=vid, label=v_label, attrs=attrs)

    def relationship_to_edge(
        self, eid: str, e_label: str, result: dict[str, Any]
    ) -> DataEdge:
        src_vid = str(result["src_vid"])
        dst_vid = str(result["dst_vid"])

        attrs: dict[str, int | float | str] = {}
        props: dict[str, Any] = result.get("props", {})

        for name, val in props.items():
            typed_val = str(val)
            if isinstance(val, int):
                typed_val = int(val)
            elif isinstance(val, float):
                typed_val = float(val)
            attrs[name] = typed_val

        return DataEdge(
            eid=eid,
            label=e_label,
            src_vid=src_vid,
            dst_vid=dst_vid,
            attrs=attrs,
        )

    @override
    @track_lru_cache_annotated
    @lru_cache
    def get_v(self, vid: str) -> DataVertex:
        query = f"""
            MATCH (v)
            WHERE elementId(v) = '{vid}'
            RETURN v, labels(v) AS v_label
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            raise RuntimeError(f"DataVertex (vid: {vid}) not found.")

        props = results[0]["v"]
        labels: list[str] = results[0]["v_label"]

        ret = self.node_to_vertex(vid, labels[0], props)

        return ret

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        query = f""" 
            MATCH (v:{v_label})
            RETURN elementId(v) as vid, v
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        vertices: list[DataVertex] = []
        for result in results:
            props = result["v"]
            vid = str(result["vid"])
            vertices.append(self.node_to_vertex(vid, v_label, props))

        return vertices

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        query = f"""
            MATCH (v:{v_label})
            WHERE {v_attr.to_neo4j_where_sub_sentence("v")}
            RETURN elementId(v) as vid, v
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        vertices: list[DataVertex] = []
        for result in results:
            props = result["v"]
            vid = str(result["vid"])
            vertices.append(self.node_to_vertex(vid, v_label, props))

        return vertices

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid(self, src_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = f""" 
            MATCH (src)-[e:{e_label}]->(dst) 
            WHERE elementId(src) = '{src_vid}' 
            RETURN
                elementId(e) AS eid,
                properties(e) AS props,
                elementId(src) AS src_vid,
                elementId(dst) AS dst_vid
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            eid = str(result["eid"])
            edges.append(self.relationship_to_edge(eid, e_label, result))

        return edges

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid(self, dst_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = f""" 
            MATCH (src)-[e:{e_label}]->(dst) 
            WHERE elementId(dst) = '{dst_vid}' 
            RETURN
                elementId(e) AS eid,
                properties(e) AS props,
                elementId(src) AS src_vid,
                elementId(dst) AS dst_vid
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            eid = str(result["eid"])
            edges.append(self.relationship_to_edge(eid, e_label, result))

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
        query = f""" 
            MATCH (src)-[e:{e_label}]->(dst) 
            WHERE elementId(src) = '{src_vid}'
            AND {e_attr.to_neo4j_where_sub_sentence("e")}
            RETURN
                elementId(e) AS eid,
                properties(e) AS props,
                elementId(src) AS src_vid,
                elementId(dst) AS dst_vid
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            eid = str(result["eid"])
            edges.append(self.relationship_to_edge(eid, e_label, result))

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
        query = f""" 
            MATCH (src)-[e:{e_label}]->(dst) 
            WHERE elementId(dst) = '{dst_vid}'
            AND {e_attr.to_neo4j_where_sub_sentence("e")} 
            RETURN
                elementId(e) AS eid,
                properties(e) AS props,
                elementId(src) AS src_vid,
                elementId(dst) AS dst_vid
        """
        results = self.execute_query(cast(LiteralString, query))
        if not results:
            return []

        edges: list[DataEdge] = []
        for result in results:
            eid = str(result["eid"])
            edges.append(self.relationship_to_edge(eid, e_label, result))

        return edges
