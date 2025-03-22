"""
配置文件
"""

SIMPLE_TEST_DB_URL = "sqlite:///data_graph.db"

LDBC_SNB_INTERACTIVE_DB_URL = "sqlite:///ldbc_sn_interactive_sf01.db"

SQLITE_SCHEMA_USE_RELATIONSHIP = False
""" SQLite 实体类定义 是否添加 `Relationship` 字段 """

SQLITE_ATTR_USE_FOREIGN_KEY = SQLITE_SCHEMA_USE_RELATIONSHIP or False
""" SQLite `Attribute` 是否使用外键约束 """

DIRECTED_EDGE_SUPPORT = True
""" 是否支持有向边 """

DBG_INSTR = True
""" 是否调试 `指令信息` """
