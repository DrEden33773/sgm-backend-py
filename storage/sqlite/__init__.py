from typing import Optional, override

from sqlmodel import Session, select

from schema import Attr, DataEdge, DataVertex, Label
from storage.abc import StorageAdapter
from storage.sqlite.db_entity import init_db


class SQLiteStorageAdapter(StorageAdapter):
    """SQLite 存储适配器"""

    def __init__(self) -> None:
        super().__init__()
        self.engine = init_db()

    @override
    def load_vertices(
        self,
        v_label: Label,
        v_attr: Optional[Attr] = None,
    ) -> list[DataVertex]:
        from storage.sqlite.db_entity import Vertex as DBVertex

        with Session(self.engine) as session:
            query = select(DBVertex).where(DBVertex.label == v_label)
            if v_attr:
                attr_type = type(v_attr).__name__
                attr_value = str(v_attr)
                query = query.where(
                    DBVertex.attr_value == attr_value,
                    DBVertex.attr_type == attr_type,
                )

            db_vertices = session.exec(query).all()
            return [
                DataVertex(
                    vid=db_vertex.vid, label=db_vertex.label, attr=db_vertex.attr
                )
                for db_vertex in db_vertices
            ]

    @override
    def load_edges(
        self,
        e_label: Label,
        e_attr: Optional[Attr] = None,
    ) -> list[DataEdge]:
        from storage.sqlite.db_entity import Edge as DBEdge

        with Session(self.engine) as session:
            query = select(DBEdge).where(DBEdge.label == e_label)
            if e_attr:
                attr_type = type(e_attr).__name__
                attr_value = str(e_attr)
                query = query.where(
                    DBEdge.attr_value == attr_value,
                    DBEdge.attr_type == attr_type,
                )

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
