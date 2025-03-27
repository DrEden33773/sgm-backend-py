import time
from pathlib import Path
from typing import Any, Callable

import polars as pl
from sqlalchemy import Engine
from sqlmodel import Session, text
from tqdm import tqdm

from storage.sqlite.db_entity import DB_Edge, DB_Vertex, init_db_with_clear

ROOT = Path(__file__).parent.absolute()
BI_DATA_DIR = ROOT / "data" / "ldbc-sn-bi-sf1"
NODES = BI_DATA_DIR / "nodes"
RELATIONSHIPS = BI_DATA_DIR / "relationships"

# 配置常量
BATCH_SIZE = 10000  # 批处理大小
COMMIT_FREQUENCY = 100000  # 提交频率


def to_typed_attrs(df: pl.DataFrame):
    """将DataFrame的列批量转换为类型化的属性字典列表"""
    # 预处理: 识别各列的类型
    result_dicts: list[dict[str, Any]] = []

    # 获取所有列名和它们的类型信息
    columns = df.columns
    typed_columns: dict[str, tuple[str, str]] = {}

    for col in columns:
        if ":" not in col:
            typed_columns[col] = ("str", col)
            continue

        if (
            col.startswith(":ID")
            or col.startswith(":START_ID")
            or col.startswith(":END_ID")
            or col == ":TYPE"
            or col == ":LABEL"
        ):
            continue

        name, type_info = col.split(":", 1)

        if type_info in ("int", "long"):
            typed_columns[col] = ("int", name)
        elif type_info == "float":
            typed_columns[col] = ("float", name)
        else:
            typed_columns[col] = ("str", name)

    # 转换为字典列表
    for row in df.rows():
        attrs: dict[str, Any] = {}
        for i, col in enumerate(columns):
            if col in typed_columns:
                type_name, attr_name = typed_columns[col]
                value = row[i]

                if value is None:
                    continue

                if type_name == "int":
                    attrs[attr_name] = int(value)
                elif type_name == "float":
                    attrs[attr_name] = float(value)
                else:
                    attrs[attr_name] = str(value)

        result_dicts.append(attrs)

    return result_dicts


def load_v(file_path: Path, session: Session) -> None:
    """批量加载节点数据"""
    df = pl.scan_csv(file_path, separator="|").collect()

    if df.height == 0:
        print(f"文件 '{file_path}' 为空, 跳过.")
        return

    scope = file_path.parent.stem

    # 提取ID和标签列
    id_col = [col for col in df.columns if col.startswith(f":ID({scope})")]
    if not id_col:
        print(f"找不到ID列 ':ID({scope})', 跳过文件 '{file_path}'.")
        return

    id_col = id_col[0]

    # 预处理数据
    ids = df.get_column(id_col).cast(pl.Utf8).to_list()
    labels = df.get_column(":LABEL").cast(pl.Utf8).to_list()

    # 移除特殊列创建属性DataFrame
    attr_df = df.drop([id_col, ":LABEL"])
    typed_attrs_list = to_typed_attrs(attr_df)

    # 批量创建顶点并添加到会话
    total = len(ids)
    batch_vertices: list[DB_Vertex] = []

    with tqdm(total=total, desc=f"加载点 '{file_path.stem}'") as bar:
        for i in range(total):
            vid = f"{scope}^{ids[i]}"
            new_vertex = DB_Vertex(vid=vid, label=labels[i], attrs=typed_attrs_list[i])
            batch_vertices.append(new_vertex)

            if len(batch_vertices) >= BATCH_SIZE or i == total - 1:
                # 批量添加到会话
                session.add_all(batch_vertices)

                # 批量加载属性
                for v in batch_vertices:
                    v.load_pending_attrs(session)

                # 检查是否需要提交
                if i >= COMMIT_FREQUENCY and i % COMMIT_FREQUENCY < BATCH_SIZE:
                    session.commit()

                batch_vertices = []

            bar.update(1)

        # 最终提交
        session.commit()


def load_e(file_path: Path, session: Session) -> None:
    """批量加载边数据"""
    df = pl.scan_csv(file_path, separator="|").collect()

    if df.height == 0:
        print(f"文件 '{file_path}' 为空, 跳过.")
        return

    path_parts = file_path.parent.stem.split("_")
    if len(path_parts) < 3:
        print(f"文件路径格式不正确: '{file_path}', 跳过.")
        return

    src_scope = path_parts[0]
    dst_scope = path_parts[2]

    # 提取源和目标ID列
    src_id_col = [
        col for col in df.columns if col.startswith(f":START_ID({src_scope})")
    ]
    dst_id_col = [col for col in df.columns if col.startswith(f":END_ID({dst_scope})")]

    if not src_id_col or not dst_id_col:
        print(f"找不到必要的ID列, 跳过文件 '{file_path}'.")
        return

    src_id_col = src_id_col[0]
    dst_id_col = dst_id_col[0]

    # 预处理数据
    src_ids = df.get_column(src_id_col).cast(pl.Utf8).to_list()
    dst_ids = df.get_column(dst_id_col).cast(pl.Utf8).to_list()
    labels = df.get_column(":TYPE").cast(pl.Utf8).to_list()

    # 移除特殊列创建属性DataFrame
    attr_df = df.drop([src_id_col, dst_id_col, ":TYPE"])
    typed_attrs_list = to_typed_attrs(attr_df)

    # 批量创建边并添加到会话
    total = len(src_ids)
    batch_edges: list[DB_Edge] = []

    # 使用字典跟踪原始边计数
    raw_eid_map: dict[str, int] = {}

    with tqdm(total=total, desc=f"加载边 '{file_path.stem}'") as bar:
        for i in range(total):
            src_vid = f"{src_scope}^{src_ids[i]}"
            dst_vid = f"{dst_scope}^{dst_ids[i]}"

            raw_eid = f"{src_vid} -> {dst_vid}"
            raw_eid_map.setdefault(raw_eid, 0)
            eid = f"{raw_eid} @ {raw_eid_map[raw_eid]}"
            raw_eid_map[raw_eid] += 1

            new_edge = DB_Edge(
                eid=eid,
                src_vid=src_vid,
                dst_vid=dst_vid,
                label=labels[i],
                attrs=typed_attrs_list[i],
            )
            batch_edges.append(new_edge)

            if len(batch_edges) >= BATCH_SIZE or i == total - 1:
                # 批量添加到会话
                session.add_all(batch_edges)

                # 批量加载属性
                for e in batch_edges:
                    e.load_pending_attrs(session)

                # 检查是否需要提交
                if i >= COMMIT_FREQUENCY and i % COMMIT_FREQUENCY < BATCH_SIZE:
                    session.commit()

                batch_edges = []

            bar.update(1)

        # 最终提交
        session.commit()


def optimize_sqlite_connection(engine: Engine):
    """优化SQLite连接设置以提高性能"""
    with engine.connect() as conn:
        conn.execute(text("PRAGMA synchronous = OFF"))  # 降低同步级别
        conn.execute(text("PRAGMA journal_mode = MEMORY"))  # 内存中的日志
        conn.execute(text("PRAGMA cache_size = 100000"))  # 增加缓存大小
        conn.execute(text("PRAGMA temp_store = MEMORY"))  # 临时表存储在内存中
        conn.execute(text("PRAGMA locking_mode = EXCLUSIVE"))  # 独占锁定模式
        conn.execute(text("PRAGMA count_changes = OFF"))  # 关闭变更计数
        conn.commit()


def process_folder(
    folder_path: Path, session: Session, process_func: Callable[[Path, Session], Any]
) -> None:
    """处理文件夹中的所有CSV文件"""
    for file_path in sorted(folder_path.glob("**/*.csv")):
        if not file_path.is_file():
            continue

        process_func(file_path, session)


if __name__ == "__main__":
    start_time = time.time()

    DB_URL = "sqlite:///ldbc_sn_bi_sf1.db"
    engine = init_db_with_clear(DB_URL, echo=False)

    # 优化SQLite连接
    optimize_sqlite_connection(engine)

    with Session(engine) as session:
        # 首先处理所有节点文件夹
        for node_folder in sorted(NODES.iterdir()):
            if node_folder.is_dir():
                process_folder(node_folder, session, load_v)

        # 然后处理所有关系文件夹
        for relationship_folder in sorted(RELATIONSHIPS.iterdir()):
            if relationship_folder.is_dir():
                process_folder(relationship_folder, session, load_e)

    end_time = time.time()
    print(f"导入完成! 总耗时: {end_time - start_time:.2f} 秒")
