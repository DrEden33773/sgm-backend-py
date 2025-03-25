from typing import Any, Optional, Sequence

from sqlalchemy import (
    Column,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Row,
    String,
    Table,
    create_engine,
)
from sqlalchemy.engine import Engine

from config import SIMPLE_TEST_SQL_DB_URL, SQLITE_ATTR_USE_FOREIGN_KEY
from schema import DataEdge, DataVertex, PatternAttr
from schema.basic import str_op_to_operator

metadata = MetaData()

# 顶点表
Vertices = Table(
    "db_vertex",
    metadata,
    Column("vid", String, primary_key=True),
    Column("label", String, index=True),
    sqlite_autoincrement=True,
)

# 边表
Edges = Table(
    "db_edge",
    metadata,
    Column("eid", String, primary_key=True),
    Column("label", String, index=True),
    Column("src_vid", String, index=True),
    Column("dst_vid", String, index=True),
    sqlite_autoincrement=True,
)

# 顶点属性表
VertexAttributes = Table(
    "vertex_attribute",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "vid",
        String,
        ForeignKey("db_vertex.vid"),
        index=True,
    )
    if SQLITE_ATTR_USE_FOREIGN_KEY
    else Column("vid", String, index=True),
    Column("key", String, index=True),
    Column("value", String),
    Column("type", String),
    sqlite_autoincrement=True,
)

# 边属性表
EdgeAttributes = Table(
    "edge_attribute",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "eid",
        String,
        ForeignKey("db_edge.eid"),
        index=True,
    )
    if SQLITE_ATTR_USE_FOREIGN_KEY
    else Column("eid", String, index=True),
    Column("key", String, index=True),
    Column("value", String),
    Column("type", String),
    sqlite_autoincrement=True,
)

# 创建索引以加速查询
Index("idx_vertex_label", Vertices.c.label)
Index("idx_edge_label", Edges.c.label)
Index("idx_edge_src_vid", Edges.c.src_vid)
Index("idx_edge_dst_vid", Edges.c.dst_vid)
Index("idx_vertex_attr_vid", VertexAttributes.c.vid)
Index("idx_edge_attr_eid", EdgeAttributes.c.eid)
Index("idx_vertex_attr_key", VertexAttributes.c.key)
Index("idx_edge_attr_key", EdgeAttributes.c.key)

type Attr = int | float | str
type AttrDict = dict[str, Attr]


def core_init_db(db_url: Optional[str] = None, echo: bool = False) -> Engine:
    """初始化数据库引擎"""
    engine = create_engine(db_url or SIMPLE_TEST_SQL_DB_URL, echo=echo)
    metadata.create_all(engine)
    return engine


def core_init_db_with_clear(db_url: Optional[str] = None, echo: bool = False) -> Engine:
    """清除并初始化数据库引擎"""
    engine = create_engine(db_url or SIMPLE_TEST_SQL_DB_URL, echo=echo)
    metadata.drop_all(engine)
    metadata.create_all(engine)
    return engine


def convert_attr_value(value: str, type_name: str) -> Attr:
    """转换属性值为正确的类型"""
    if type_name == "int":
        return int(value)
    elif type_name == "float":
        return float(value)
    return value


def row_to_attrs_dict(
    rows: Optional[Sequence[Row[Any]]],
) -> AttrDict:
    """将属性行转换为属性字典"""
    return (
        {row.key: convert_attr_value(row.value, row.type) for row in rows}
        if rows
        else {}
    )


def row_to_vertex(row: Row[Any], attrs: dict[str, Any] = {}) -> DataVertex:
    """将查询结果转换为DataVertex"""
    return DataVertex(vid=row.vid, label=row.label, attrs=attrs or {})


def row_to_edge(row: Row[Any], attrs: dict[str, Any] = {}) -> DataEdge:
    """将查询结果转换为DataEdge"""
    return DataEdge(
        eid=row.eid,
        label=row.label,
        src_vid=row.src_vid,
        dst_vid=row.dst_vid,
        attrs=attrs or {},
    )


def attr_satisfies_pattern(
    attr_rows: Sequence[Row[Any]], pattern_attr: PatternAttr
) -> bool:
    """检查属性是否满足模式条件"""
    if not attr_rows:
        return False

    for row in attr_rows:
        if row.key == pattern_attr.key:
            operator = str_op_to_operator(pattern_attr.op)
            value = convert_attr_value(row.value, row.type)
            if type(value) is type(pattern_attr.value) and operator(
                value, pattern_attr.value
            ):
                return True
    return False
