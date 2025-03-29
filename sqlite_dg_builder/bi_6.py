from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear


class BI6Builder:
    def __init__(self) -> None:
        posts = [DataVertex(f"post{i}", "Post") for i in [1, 2, 3]]
        tags = [
            DataVertex(f"tag{i}", "Tag", attrs={"name": "The_Mouse_and_the_Mask"})
            for i in [1]
        ]
        persons = [DataVertex(f"person{i}", "Person") for i in [1, 2, 3, 4]]

        has_tag = [
            DataEdge("1", "hasTag", "post1", "tag1"),
        ]
        has_creator = [
            DataEdge("2", "hasCreator", "post1", "person1"),
            DataEdge("4", "hasCreator", "post2", "person2"),
            DataEdge("5", "hasCreator", "post3", "person2"),
        ]
        likes = [
            DataEdge("3", "likes", "person2", "post1"),
            DataEdge("6", "likes", "person3", "post2"),
            DataEdge("7", "likes", "person4", "post3"),
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
            for e in (has_tag + has_creator + likes)
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
