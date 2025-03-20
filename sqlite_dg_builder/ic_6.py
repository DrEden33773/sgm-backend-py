from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear


class IC6Builder:
    def __init__(self) -> None:
        posts = [DataVertex(str(vid), "Post") for vid in [1, 2, 3]]
        tags = [
            DataVertex("4", "Tag", attrs={"name": "ComputerScience", "color": "red"}),
            DataVertex("5", "Tag", attrs={"name": "Engineering", "color": "blue"}),
            DataVertex("6", "Tag", attrs={"name": "Art", "color": "green"}),
        ] + [DataVertex(str(vid), "Tag") for vid in [7, 8]]
        persons = [
            DataVertex("9", "Person", attrs={"id": 123}),
            DataVertex("10", "Person", attrs={"id": 456}),
        ] + [DataVertex(str(vid), "Person") for vid in [11, 12, 13, 14, 15]]

        has_tag = [
            DataEdge("a", "hasTag", "1", "4"),
            DataEdge("b", "hasTag", "1", "5"),
            DataEdge("c", "hasTag", "1", "6"),
            DataEdge("d", "hasTag", "2", "7"),
            DataEdge("e", "hasTag", "2", "8"),
        ]
        has_creator = [
            DataEdge("f", "hasCreator", "1", "11"),
            DataEdge("g", "hasCreator", "2", "11"),
            DataEdge("h", "hasCreator", "3", "12"),
        ]
        knows = [
            DataEdge("i", "knows", "9", "11"),
            DataEdge("j", "knows", "11", "9"),
            DataEdge("k", "knows", "10", "11"),
            DataEdge("m", "knows", "11", "10"),
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
