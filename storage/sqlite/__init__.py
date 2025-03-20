from functools import lru_cache
from typing import Optional, override

from sqlmodel import Session, select

from schema import DataEdge, DataVertex, Label, PatternAttr
from storage.abc import StorageAdapter
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db
from utils.tracked_lru_cache import track_lru_cache_annotated


class SQLiteStorageAdapter(StorageAdapter):
    """SQLite 存储适配器"""

    def __init__(self, db_url: Optional[str] = None) -> None:
        super().__init__()
        self.engine = init_db() if not db_url else init_db(db_url)

    @override
    @track_lru_cache_annotated
    @lru_cache
    def get_v(self, vid: str) -> DataVertex:
        query = select(DB_Vertex).where(DB_Vertex.vid == vid)
        with Session(self.engine) as session:
            db_vertex = session.exec(query).first()
            if not db_vertex:
                raise RuntimeError(f"DataVertex (vid: {vid}) not found.")
            attrs = db_vertex.get_attributes(session)
        return DataVertex(vid=db_vertex.vid, label=db_vertex.label, attrs=attrs)

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        query = select(DB_Vertex).where(DB_Vertex.label == v_label)
        with Session(self.engine) as session:
            db_vertices = session.exec(query).all()
            return [
                DataVertex(
                    vid=db_vertex.vid,
                    label=db_vertex.label,
                    attrs=db_vertex.get_attributes(session),
                )
                for db_vertex in db_vertices
            ]

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        query = select(DB_Vertex).where(DB_Vertex.label == v_label)
        with Session(self.engine) as session:
            db_vertices = session.exec(query).all()
            return [
                DataVertex(
                    vid=db_vertex.vid,
                    label=db_vertex.label,
                    attrs=db_vertex.get_attributes(session),
                )
                for db_vertex in db_vertices
                if v_attr.is_data_attrs_satisfied(db_vertex.get_attributes(session))
            ]

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e(self, e_label: Label) -> list[DataEdge]:
        query = select(DB_Edge).where(DB_Edge.label == e_label)
        with Session(self.engine) as session:
            db_edges = session.exec(query).all()
            return [
                DataEdge(
                    eid=db_edge.eid,
                    label=db_edge.label,
                    src_vid=db_edge.src_vid,
                    dst_vid=db_edge.dst_vid,
                    attrs=db_edge.get_attributes(session),
                )
                for db_edge in db_edges
            ]

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_with_attr(
        self,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = select(DB_Edge).where(DB_Edge.label == e_label)
        with Session(self.engine) as session:
            db_edges = session.exec(query).all()
        return [
            DataEdge(
                eid=db_edge.eid,
                label=db_edge.label,
                src_vid=db_edge.src_vid,
                dst_vid=db_edge.dst_vid,
                attrs=db_edge.get_attributes(session),
            )
            for db_edge in db_edges
            if e_attr.is_data_attrs_satisfied(db_edge.get_attributes(session))
        ]
