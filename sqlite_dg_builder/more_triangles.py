from sqlmodel import Session

from schema import DataEdge, DataVertex
from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear


class MoreTriangleDgBuilder:
    def __init__(self) -> None:
        RED = "Red"
        GREEN = "Green"
        BLUE = "Blue"
        EDGE = "Edge"

        red_vertices = [DataVertex(str(vid), RED) for vid in [1, 4, 9, 10]]
        green_vertices = [DataVertex(str(vid), GREEN) for vid in [2, 5, 7]]
        blue_vertices = [DataVertex(str(vid), BLUE) for vid in [3, 6, 8]]

        vertices = red_vertices + green_vertices + blue_vertices
        raw_edges = [
            DataEdge("a", EDGE, "1", "2"),
            DataEdge("b", EDGE, "2", "3"),
            DataEdge("c", EDGE, "3", "1"),
            DataEdge("d", EDGE, "4", "5"),
            DataEdge("e", EDGE, "5", "6"),
            DataEdge("f", EDGE, "6", "4"),
            DataEdge("g", EDGE, "4", "7"),
            DataEdge("j", EDGE, "7", "8"),
            DataEdge("h", EDGE, "8", "4"),
            DataEdge("i", EDGE, "1", "4"),
            DataEdge("k", EDGE, "10", "5"),
            DataEdge("m", EDGE, "6", "10"),
        ]
        # 经过和查询计划的比对, raw_edge 每条边的方向都是反的, 全部反转
        edges = [e.reversed() for e in raw_edges]

        self.db_vertices = [DB_Vertex(vid=v.vid, label=v.label) for v in vertices]
        self.db_edges = [
            DB_Edge(
                eid=e.eid,
                src_vid=e.src_vid,
                dst_vid=e.dst_vid,
                label=e.label,
            )
            for e in edges
        ]
        self.engine = init_db_with_clear()

    def build(self):
        with Session(self.engine) as session:
            session.add_all(self.db_vertices)
            session.add_all(self.db_edges)
            session.commit()
