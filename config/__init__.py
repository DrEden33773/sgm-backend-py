"""
配置文件
"""

SQLITE_DB_URL = "sqlite:///data_graph.db"
""" SQLite 数据库连接字符串 """

SQLITE_SCHEMA_USE_RELATIONSHIP = False
""" SQLite 实体类定义 是否添加 `Relationship` 字段 """

SQLITE_ATTR_USE_FOREIGN_KEY = SQLITE_SCHEMA_USE_RELATIONSHIP or False
""" SQLite `Attribute` 是否使用外键约束 """

DIRECTED_EDGE_SUPPORT = True
""" 是否支持有向边 """

DBG_INSTR = False
""" 是否调试 `指令信息` """
