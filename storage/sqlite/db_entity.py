from typing import Optional

from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select

from config import DB_URL

type Attr = int | float | str
type AttrDict = dict[str, Attr]


class ConstraintBase(SQLModel):
    label: str
    attr_value: Optional[str] = Field(default=None, nullable=True)
    attr_type: Optional[str] = Field(default=None, nullable=True)

    def __init__(self, label: str, attr: Optional[Attr]):
        attr_value = None
        attr_type = None
        if attr:
            attr_value = str(attr)
            attr_type = type(attr).__name__
        super().__init__(label=label, attr_value=attr_value, attr_type=attr_type)

    @property
    def attr(self) -> Optional[Attr]:
        """获取属性值"""

        if self.attr_value is None:
            return None
        if self.attr_type == "int":
            return int(self.attr_value)
        elif self.attr_type == "float":
            return float(self.attr_value)
        else:
            return self.attr_value


class Vertex(ConstraintBase, table=True):
    """顶点"""

    vid: str = Field(primary_key=True)

    def __init__(self, vid: str, label: str, attr: Optional[Attr] = None):
        super().__init__(label=label, attr=attr)
        self.vid = vid


class Edge(ConstraintBase, table=True):
    """边 (起始点 vid 加索引, 但是不用外键约束)"""

    eid: str = Field(primary_key=True)
    src_vid: str = Field(index=True)
    dst_vid: str = Field(index=True)

    def __init__(
        self,
        eid: str,
        src_vid: str,
        dst: str,
        label: str,
        attr: Optional[Attr] = None,
    ):
        super().__init__(label=label, attr=attr)
        self.eid = eid
        self.src_vid = src_vid
        self.dst_vid = dst


def init_db():
    engine = create_engine(DB_URL, echo=False)
    SQLModel.metadata.create_all(engine)
    return engine


def init_db_with_clear():
    engine = create_engine(DB_URL, echo=False)
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    return engine


""" ==================== 支持 `多属性` 的顶点和边 ==================== """


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


class DbVertex(SQLModel, table=True):
    """顶点 (没有属性)"""

    class Meta:
        tablename: str = "vertices"

    vid: str = Field(primary_key=True)
    label: str = Field(index=True)
    attributes: list["VertexAttribute"] = Relationship(back_populates="vertex")

    def __init__(self, vid: str, label: str, attrs: Optional[AttrDict] = None) -> None:
        super().__init__(vid=vid, label=label)
        self._pending_attrs: AttrDict = attrs or {}

    def add_attribute(
        self, session: Session, key: str, value: Attr
    ) -> "VertexAttribute":
        """添加属性"""

        attr_type = type(value).__name__
        attr_value = str(value)

        # 先检查是否存在
        stmt = select(VertexAttribute).where(
            (VertexAttribute.vid == self.vid) & (VertexAttribute.key == key)
        )
        existing_attr = session.exec(stmt).first()

        if existing_attr:
            # 如果存在, 更新属性值
            existing_attr.value = attr_value
            existing_attr.type = attr_type
            return existing_attr
        else:
            attr = VertexAttribute(
                vid=self.vid, key=key, value=attr_value, type=attr_type
            )
            session.add(attr)
            return attr

    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(VertexAttribute).where(
            (VertexAttribute.vid == self.vid) & (VertexAttribute.key == key)
        )
        attr = session.exec(stmt).first()

        if attr:
            return attr.typed_value
        return None


class DbEdge(SQLModel, table=True):
    """边 (没有属性)"""

    class Meta:
        tablename: str = "edges"

    eid: str = Field(primary_key=True)
    label: str = Field(index=True)
    src_vid: str = Field(index=True)
    dst_vid: str = Field(index=True)
    attributes: list["EdgeAttribute"] = Relationship(back_populates="edge")

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

    def add_attribute(self, session: Session, key: str, value: Attr) -> "EdgeAttribute":
        """添加属性"""

        attr_type = type(value).__name__
        attr_value = str(value)

        # 先检查是否存在
        stmt = select(EdgeAttribute).where(
            (EdgeAttribute.eid == self.eid) & (EdgeAttribute.key == key)
        )
        existing_attr = session.exec(stmt).first()

        if existing_attr:
            # 如果存在, 更新属性值
            existing_attr.value = attr_value
            existing_attr.type = attr_type
            return existing_attr
        else:
            attr = EdgeAttribute(
                eid=self.eid, key=key, value=attr_value, type=attr_type
            )
            session.add(attr)
            return attr

    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(EdgeAttribute).where(
            (EdgeAttribute.eid == self.eid) & (EdgeAttribute.key == key)
        )
        attr = session.exec(stmt).first()

        if attr:
            return attr.typed_value
        return None


class VertexAttribute(BaseAttribute, table=True):
    """顶点属性"""

    class Meta:
        tablename: str = "vertex_attributes"

    vid: str = Field(index=True)
    vertex: DbVertex = Relationship(back_populates="attributes")


class EdgeAttribute(BaseAttribute, table=True):
    """边属性"""

    class Meta:
        tablename: str = "edge_attributes"

    eid: str = Field(index=True)
    edge: DbEdge = Relationship(back_populates="attributes")
