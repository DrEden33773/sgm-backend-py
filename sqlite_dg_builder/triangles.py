from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import init_db_with_clear


class TriangleDgBuilder:
    def __init__(self) -> None:
        RED = "red"
        GREEN = "green"
        BLUE = "blue"
        TO = "to"

        red_vertices = [DataVertex(str(vid), RED) for vid in [1, 2]]
        green_vertices = [DataVertex(str(vid), GREEN) for vid in [5]]
        blue_vertices = [DataVertex(str(vid), BLUE) for vid in [3, 4]]

        self.vertices = red_vertices + green_vertices + blue_vertices
        self.edges = [
            DataEdge("a", TO, "2", "3"),
            DataEdge("c", TO, "3", "5"),
            DataEdge("b", TO, "5", "2"),
            DataEdge("d", TO, "5", "4"),
        ]

        from storage.sqlite.db_entity import Edge, Vertex

        self.db_vertices = [
            Vertex(vid=vertex.vid, label=vertex.label) for vertex in self.vertices
        ]
        self.db_edges = [
            Edge(
                eid=edge.eid,
                src_vid=edge.src_vid,
                dst=edge.dst_vid,
                label=edge.label,
            )
            for edge in self.edges
        ]

        self.engine = init_db_with_clear()

    def build(self):
        with Session(self.engine) as session:
            session.add_all(self.db_vertices)
            session.add_all(self.db_edges)
            session.commit()
