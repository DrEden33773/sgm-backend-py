from functools import lru_cache
from typing import Optional

from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select

from config import (
    SIMPLE_TEST_DB_URL,
    SQLITE_ATTR_USE_FOREIGN_KEY,
    SQLITE_SCHEMA_USE_RELATIONSHIP,
)
from schema import PatternAttr
from schema.basic import str_op_to_operator
from utils.tracked_lru_cache import track_lru_cache_annotated

type Attr = int | float | str
type AttrDict = dict[str, Attr]


class BaseAttribute(SQLModel):
    id: Optional[int] = Field(default=None, primary_key=True)
    key: str = Field(index=True)
    value: str
    type: str

    __table_args__ = {"sqlite_autoincrement": True}

    @property
    def typed_value(self) -> Attr:
        """获取属性值"""
        if self.type == "int":
            return int(self.value)
        elif self.type == "float":
            return float(self.value)
        else:
            return self.value

    def could_satisfy_pattern_attr(self, pattern_attr: PatternAttr) -> bool:
        if self.key != pattern_attr.key:
            return False
        if self.type != type(pattern_attr.value).__name__:
            return False
        operator = str_op_to_operator(pattern_attr.op)
        return operator(self.typed_value, pattern_attr.value)


class DB_Vertex(SQLModel, table=True):
    """顶点 (没有属性)"""

    vid: str = Field(primary_key=True)
    label: str = Field(index=True)

    def __init__(self, vid: str, label: str, attrs: Optional[AttrDict] = None) -> None:
        super().__init__(vid=vid, label=label)
        self._pending_attrs: AttrDict = attrs or {}
        if SQLITE_SCHEMA_USE_RELATIONSHIP:
            self.attributes: list[Vertex_Attribute] = Relationship(
                back_populates="vertex"
            )

    def load_pending_attrs(self, session: Session):
        """加载待添加的属性到数据库中"""

        attributes: list[Vertex_Attribute] = []
        for key, value in self._pending_attrs.items():
            attr = Vertex_Attribute(
                vid=self.vid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)
            attributes.append(attr)

        self._pending_attrs.clear()  # 清除待添加的属性
        self.get_attribute.cache_clear()  # 清除缓存
        self.get_attributes.cache_clear()  # 清除缓存
        return attributes

    @track_lru_cache_annotated
    @lru_cache
    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(Vertex_Attribute).where(
            (Vertex_Attribute.vid == self.vid) & (Vertex_Attribute.key == key)
        )
        attr = session.exec(stmt).first()
        return attr.typed_value if attr else None

    @track_lru_cache_annotated
    @lru_cache
    def get_attributes(self, session: Session) -> AttrDict:
        """获取所有属性值"""

        stmt = select(Vertex_Attribute).where(Vertex_Attribute.vid == self.vid)
        attrs = session.exec(stmt).all()
        return {attr.key: attr.typed_value for attr in attrs} if attrs else {}

    def __hash__(self) -> int:
        return hash(self.vid)


class DB_Edge(SQLModel, table=True):
    """边 (没有属性)"""

    eid: str = Field(primary_key=True)
    label: str = Field(index=True)
    src_vid: str = Field(index=True)
    dst_vid: str = Field(index=True)

    def __init__(
        self,
        eid: str,
        src_vid: str,
        dst_vid: str,
        label: str,
        attrs: Optional[AttrDict] = None,
    ) -> None:
        super().__init__(eid=eid, src_vid=src_vid, dst_vid=dst_vid, label=label)
        self._pending_attrs: AttrDict = attrs or {}
        if SQLITE_SCHEMA_USE_RELATIONSHIP:
            self.attributes: list[Edge_Attribute] = Relationship(back_populates="edge")

    def load_pending_attrs(self, session: Session):
        """加载待添加的属性到数据库中"""

        attributes: list[Edge_Attribute] = []

        for key, value in self._pending_attrs.items():
            attr = Edge_Attribute(
                eid=self.eid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)
            attributes.append(attr)

        self._pending_attrs.clear()  # 清除待添加的属性
        self.get_attribute.cache_clear()  # 清除缓存
        self.get_attributes.cache_clear()  # 清除缓存
        return attributes

    @track_lru_cache_annotated
    @lru_cache
    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(Edge_Attribute).where(
            (Edge_Attribute.eid == self.eid) & (Edge_Attribute.key == key)
        )
        attr = session.exec(stmt).first()
        return attr.typed_value if attr else None

    @track_lru_cache_annotated
    @lru_cache
    def get_attributes(self, session: Session) -> AttrDict:
        """获取所有属性值"""

        stmt = select(Edge_Attribute).where(Edge_Attribute.eid == self.eid)
        attrs = session.exec(stmt).all()
        return {attr.key: attr.typed_value for attr in attrs} if attrs else {}

    def __hash__(self) -> int:
        return hash(self.eid)


class Vertex_Attribute(BaseAttribute, table=True):
    """顶点属性"""

    vid: str = Field(
        index=True, foreign_key="db_vertex.vid" if SQLITE_ATTR_USE_FOREIGN_KEY else None
    )

    def __post_init__(self):
        if SQLITE_SCHEMA_USE_RELATIONSHIP:
            self.vertex: DB_Vertex = Relationship(back_populates="attributes")

    def __hash__(self) -> int:
        vid_hashed = hash(self.vid)
        return vid_hashed ^ hash(self.id) if self.id else vid_hashed


class Edge_Attribute(BaseAttribute, table=True):
    """边属性"""

    eid: str = Field(
        index=True, foreign_key="db_edge.eid" if SQLITE_ATTR_USE_FOREIGN_KEY else None
    )

    def __post_init__(self):
        if SQLITE_SCHEMA_USE_RELATIONSHIP:
            self.edge: DB_Edge = Relationship(back_populates="attributes")

    def __hash__(self) -> int:
        eid_hashed = hash(self.eid)
        return eid_hashed ^ hash(self.id) if self.id else eid_hashed


def init_db(db_url: Optional[str] = None, echo: bool = False):
    engine = create_engine(SIMPLE_TEST_DB_URL if not db_url else db_url, echo=echo)
    SQLModel.metadata.create_all(engine)
    return engine


def init_db_with_clear(db_url: Optional[str] = None, echo: bool = False):
    engine = create_engine(SIMPLE_TEST_DB_URL if not db_url else db_url, echo=echo)
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    return engine
