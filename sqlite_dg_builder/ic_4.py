from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear


class IC4Builder:
    def __init__(self) -> None:
        posts = [DataVertex(str(vid), "Post") for vid in [1, 2]]
        tags = [DataVertex(str(vid), "Tag") for vid in [3]]
        persons = [
            DataVertex("123", "Person", attrs={"id": 123}),
            DataVertex("456", "Person", attrs={"id": 456}),
            DataVertex("246", "Person", attrs={"id": 246}),
        ]

        has_tag = [
            DataEdge("e", "hasTag", "1", "3"),
            DataEdge("f", "hasTag", "2", "3"),
        ]
        has_creator = [
            DataEdge("c", "hasCreator", "1", "123"),
            DataEdge("d", "hasCreator", "2", "456"),
        ]
        knows = [
            DataEdge("a", "knows", "123", "246"),
            DataEdge("b", "knows", "456", "246"),
        ]

        self.db_vertices = [
            DB_Vertex(vid=v.vid, label=v.label, attrs=v.attrs)
            for v in (posts + tags + persons)
        ]
        self.db_edges = [
            DB_Edge(
                eid=e.eid,
                src_vid=e.src_vid,
                dst_vid=e.dst_vid,
                label=e.label,
                attrs=e.attrs,
            )
            for e in (has_tag + has_creator + knows)
        ]
        self.engine = init_db_with_clear()

    def build(self):
        with Session(self.engine) as session:
            session.add_all(self.db_vertices)
            session.add_all(self.db_edges)
            for v in self.db_vertices:
                v.load_pending_attrs(session)
            for e in self.db_edges:
                e.load_pending_attrs(session)
            session.commit()
