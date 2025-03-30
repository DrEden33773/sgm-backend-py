from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear


class IC5Builder:
    def __init__(self) -> None:
        posts = [
            DataVertex("4", "Post"),
            DataVertex("44", "Post"),
        ]
        forums = [
            DataVertex("3", "Forum"),
            DataVertex("33", "Forum"),
        ]
        persons = [
            DataVertex("94", "Person", attrs={"id": 94}),
            DataVertex("1", "Person", attrs={"id": 1}),
            DataVertex("2", "Person", attrs={"id": 2}),
        ]

        knows = [
            DataEdge("a", "knows", "94", "1"),
            DataEdge("b", "knows", "94", "2"),
        ]
        container_of = [
            DataEdge("c", "containerOf", "3", "4"),
            DataEdge("c*", "containerOf", "3", "44"),
            DataEdge("cc", "containerOf", "33", "44"),
            DataEdge("cc*", "containerOf", "33", "4"),
        ]
        has_member = [
            DataEdge("d", "hasMember", "3", "1"),
            DataEdge("dd", "hasMember", "33", "1"),
            DataEdge("e", "hasMember", "3", "2"),
            DataEdge("ee", "hasMember", "33", "2"),
        ]
        has_creator = [
            DataEdge("f", "hasCreator", "4", "1"),
            DataEdge("ff", "hasCreator", "44", "1"),
            DataEdge("g", "hasCreator", "4", "2"),
            DataEdge("gg", "hasCreator", "44", "2"),
        ]

        self.db_vertices = [
            DB_Vertex(vid=v.vid, label=v.label, attrs=v.attrs)
            for v in (posts + forums + persons)
        ]
        self.db_edges = [
            DB_Edge(
                eid=e.eid,
                src_vid=e.src_vid,
                dst_vid=e.dst_vid,
                label=e.label,
                attrs=e.attrs,
            )
            for e in (has_member + has_creator + knows + container_of)
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
