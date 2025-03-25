from functools import lru_cache
from typing import Any, Optional, cast, override

from sqlalchemy import Connection, Row
from sqlalchemy import select as core_select
from sqlmodel import Session, select

from config import USE_CORE_MODE
from schema import DataEdge, DataVertex, Label, PatternAttr, Vid
from storage.abc import StorageAdapter
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db
from storage.sqlite.db_schema import (
    EdgeAttributes,
    Edges,
    VertexAttributes,
    Vertices,
    row_to_attrs_dict,
    row_to_edge,
    row_to_vertex,
)
from utils.tracked_lru_cache import track_lru_cache_annotated


class SQLiteStorageAdapter(StorageAdapter):
    """SQLite 存储适配器"""

    def __init__(self, db_url: Optional[str] = None) -> None:
        super().__init__()
        self.engine = init_db() if not db_url else init_db(db_url)
        self.connection = self.engine.connect() if USE_CORE_MODE else None

    def __del__(self) -> None:
        self.connection.close() if self.connection else None

    def core_get_v(self, vid: Vid) -> DataVertex:
        conn = cast(Connection, self.connection)
        v_query = core_select(Vertices).where(Vertices.c.vid == vid)
        v_row = conn.execute(v_query).first()
        if not v_row:
            raise RuntimeError(f"DataVertex (vid: {vid}) not found.")

        attr_query = core_select(VertexAttributes).where(VertexAttributes.c.vid == vid)
        attr_rows = conn.execute(attr_query).all()
        attrs = row_to_attrs_dict(attr_rows)
        return row_to_vertex(v_row, attrs)

    @override
    @track_lru_cache_annotated
    @lru_cache
    def get_v(self, vid: Vid) -> DataVertex:
        if self.connection:
            return self.core_get_v(vid)

        query = select(DB_Vertex).where(DB_Vertex.vid == vid)
        with Session(self.engine) as session:
            db_vertex = session.exec(query).first()
            if not db_vertex:
                raise RuntimeError(f"DataVertex (vid: {vid}) not found.")
            attrs = db_vertex.get_attributes(session)
        return DataVertex(vid=db_vertex.vid, label=db_vertex.label, attrs=attrs)

    def core_load_v(self, v_label: Label) -> list[DataVertex]:
        conn = cast(Connection, self.connection)
        v_query = core_select(Vertices).where(Vertices.c.label == v_label)
        v_rows = conn.execute(v_query).all()
        if not v_rows:
            return []

        v_ids = (row.vid for row in v_rows)
        attr_query = core_select(VertexAttributes).where(
            VertexAttributes.c.vid.in_(v_ids)
        )
        attr_rows = conn.execute(attr_query).all()

        attr_by_vid: dict[Vid, list[Row[Any]]] = {}
        for row in attr_rows:
            attr_by_vid.setdefault(row.vid, []).append(row)

        return [
            row_to_vertex(row, row_to_attrs_dict(attr_by_vid.get(row.vid, [])))
            for row in v_rows
        ]

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        if USE_CORE_MODE:
            return self.core_load_v(v_label)

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

    def core_load_e(
        self, e_label: Label, e_attr: Optional[PatternAttr]
    ) -> list[DataEdge]:
        conn = cast(Connection, self.connection)
        e_query = core_select(Edges).where(Edges.c.label == e_label)
        e_rows = conn.execute(e_query).all()
        if not e_rows:
            return []

        e_ids = (row.eid for row in e_rows)
        attr_query = core_select(EdgeAttributes).where(EdgeAttributes.c.eid.in_(e_ids))
        attr_rows = conn.execute(attr_query).all()

        attr_by_eid: dict[Vid, list[Row[Any]]] = {}
        for row in attr_rows:
            attr_by_eid.setdefault(row.eid, []).append(row)

        return [
            row_to_edge(row, row_to_attrs_dict(attr_by_eid.get(row.eid, [])))
            for row in e_rows
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
    def load_e_by_src_vid(self, src_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = (
            select(DB_Edge)
            .where(DB_Edge.label == e_label)
            .where(DB_Edge.src_vid == src_vid)
        )
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
    def load_e_by_dst_vid(self, dst_vid: Vid, e_label: Label) -> list[DataEdge]:
        query = (
            select(DB_Edge)
            .where(DB_Edge.label == e_label)
            .where(DB_Edge.dst_vid == dst_vid)
        )
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

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid_with_attr(
        self,
        src_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = (
            select(DB_Edge)
            .where(DB_Edge.label == e_label)
            .where(DB_Edge.src_vid == src_vid)
        )
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

    @override
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid_with_attr(
        self,
        dst_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        query = (
            select(DB_Edge)
            .where(DB_Edge.label == e_label)
            .where(DB_Edge.dst_vid == dst_vid)
        )
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
