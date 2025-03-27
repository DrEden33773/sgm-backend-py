from pathlib import Path
from typing import Any

import polars as pl
from sqlmodel import Session
from tqdm import tqdm

from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear

ROOT = Path(__file__).parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes" / "all"
RELATIONSHIPS = TEST_DATASET / "relationships" / "all"

raw_eid_cnt: dict[str, int] = {}


def to_typed_attrs(attrs: dict[str, Any]):
    result: dict[str, str | int | float] = {}
    for key, value in attrs.items():
        if ":" not in key:
            result[key] = str(value)
            continue
        name = key.split(":")[0]
        type_ = key.split(":")[1]
        match type_:
            case "int" | "long":
                result[name] = int(value)
            case "float":
                result[name] = float(value)
            case _:
                result[name] = str(value)
    return result


def load_v(file_path: Path, session: Session):
    global unique_vid_cnt

    df = pl.read_csv(file_path, separator="|")
    columns = df.columns
    scope = file_path.stem.split("_")[0].capitalize()
    new_vertices: list[DB_Vertex] = []

    for i in range(len(df)):
        row = df.row(i)
        attrs = {name: value for name, value in zip(columns[1:], row[1:])}
        vid = f"{scope}^{row[0]}"
        label = str(attrs.pop(":LABEL"))
        typed_attrs = to_typed_attrs(attrs)
        new_vertex = DB_Vertex(vid=vid, label=label, attrs=typed_attrs)
        new_vertices.append(new_vertex)

    with tqdm(total=len(new_vertices), desc=f"加载点信息: '{file_path}'") as bar:
        for v in new_vertices:
            session.add(v)
            v.load_pending_attrs(session)
            bar.update(1)
    session.commit()


def load_e(file_path: Path, session: Session):
    global raw_eid_cnt

    df = pl.read_csv(file_path, separator="|")
    columns = df.columns
    src_scope = file_path.stem.split("_")[0].capitalize()
    dst_scope = file_path.stem.split("_")[2].capitalize()
    new_edges: list[DB_Edge] = []

    for i in range(len(df)):
        row = df.row(i)
        attrs = {name: value for name, value in zip(columns[2:], row[2:])}
        src_vid = f"{src_scope}^{row[0]}"
        dst_vid = f"{dst_scope}^{row[1]}"

        raw_eid = f"{src_vid} -> {dst_vid}"
        raw_eid_cnt.setdefault(raw_eid, 0)
        eid = f"{raw_eid} @ {raw_eid_cnt[raw_eid]}"
        raw_eid_cnt[raw_eid] += 1

        label = attrs.pop(":TYPE")
        typed_attrs = to_typed_attrs(attrs)
        new_edge = DB_Edge(
            eid=eid,
            src_vid=src_vid,
            dst_vid=dst_vid,
            label=label,
            attrs=typed_attrs,
        )
        new_edges.append(new_edge)

    with tqdm(total=len(new_edges), desc=f"加载边信息: '{file_path}'") as bar:
        for e in new_edges:
            session.add(e)
            e.load_pending_attrs(session)
            bar.update(1)
    session.commit()


if __name__ == "__main__":
    DB_URL = "sqlite:///ldbc_sn_interactive_sf01.db"
    engine = init_db_with_clear(DB_URL, echo=False)
    with Session(engine) as session:
        for node_file in NODES.iterdir():
            if node_file.suffix != ".csv":
                continue
            load_v(node_file, session)
        for relationship_file in RELATIONSHIPS.iterdir():
            if relationship_file.suffix != ".csv":
                continue
            load_e(relationship_file, session)
