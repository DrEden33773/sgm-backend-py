from functools import lru_cache
from typing import override

from sqlmodel import Session, select

from schema import DataEdge, DataVertex, Label, PatternAttr
from storage.abc import StorageAdapter
from storage.sqlite.db_entity import Edge as DBEdge
from storage.sqlite.db_entity import Vertex as DBVertex
from storage.sqlite.db_entity import init_db


class SQLiteStorageAdapter(StorageAdapter):
    """SQLite 存储适配器"""

    def __init__(self) -> None:
        super().__init__()
        self.engine = init_db()

    @override
    @lru_cache
    def get_v(self, vid: str) -> DataVertex:
        query = select(DBVertex).where(DBVertex.vid == vid)
        with Session(self.engine) as session:
            db_vertex = session.exec(query).first()
        if not db_vertex:
            raise RuntimeError(f"DataVertex (vid: {vid}) not found.")
        return DataVertex(vid=db_vertex.vid, label=db_vertex.label, attr=db_vertex.attr)

    @override
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        query = select(DBVertex).where(DBVertex.label == v_label)
        with Session(self.engine) as session:
            db_vertices = session.exec(query).all()
        return [
            DataVertex(vid=db_vertex.vid, label=db_vertex.label, attr=db_vertex.attr)
            for db_vertex in db_vertices
        ]

    @override
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        query = (
            select(DBVertex)
            .where(DBVertex.label == v_label)
            .where(
                DBEdge.attr_value == str(v_attr),
                DBEdge.attr_type == type(v_attr).__name__,
            )
        )
        with Session(self.engine) as session:
            db_vertices = session.exec(query).all()
        return [
            DataVertex(vid=db_vertex.vid, label=db_vertex.label, attr=db_vertex.attr)
            for db_vertex in db_vertices
        ]

    @override
    @lru_cache
    def load_e(self, e_label: Label) -> list[DataEdge]:
        query = select(DBEdge).where(DBEdge.label == e_label)
        with Session(self.engine) as session:
            db_edges = session.exec(query).all()
        return [
            DataEdge(
                eid=db_edge.eid,
                label=db_edge.label,
                src_vid=db_edge.src_vid,
                dst_vid=db_edge.dst_vid,
                attr=db_edge.attr,
            )
            for db_edge in db_edges
        ]

    @override
    @lru_cache
    def load_e_with_attr(
        self,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = (
            select(DBEdge)
            .where(DBEdge.label == e_label)
            .where(
                DBEdge.attr_value == str(e_attr),
                DBEdge.attr_type == type(e_attr).__name__,
            )
        )
        with Session(self.engine) as session:
            db_edges = session.exec(query).all()
        return [
            DataEdge(
                eid=db_edge.eid,
                label=db_edge.label,
                src_vid=db_edge.src_vid,
                dst_vid=db_edge.dst_vid,
                attr=db_edge.attr,
            )
            for db_edge in db_edges
        ]
