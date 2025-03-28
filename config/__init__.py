"""
配置文件
"""

from pathlib import Path
from typing import Literal

SCRIPT_DIR = Path(__file__).parent.parent.absolute()

SIMPLE_TEST_SQL_DB_URL = "sqlite:///simple_test_data_graph.db"
LDBC_SNB_INTERACTIVE_SQL_DB_URL = "sqlite:///ldbc_sn_interactive_sf01.db"

WHICH_DB: Literal["SQLite", "Neo4j"] = "Neo4j"
""" 使用的数据库类型 ("SQLite" / "Neo4j") """

SQLITE_SCHEMA_USE_RELATIONSHIP = False
""" SQLite 实体类定义 是否添加 `Relationship` 字段 """

SQLITE_ATTR_USE_FOREIGN_KEY = SQLITE_SCHEMA_USE_RELATIONSHIP or False
""" SQLite `Attribute` 是否使用外键约束 """

USE_CORE_MODE = False
""" 是否使用 `SQLAlchemy Core` """

DIRECTED_EDGE_SUPPORT = True
""" 是否支持有向边 """

DBG_INSTR = True
""" 是否调试 `指令信息` """
