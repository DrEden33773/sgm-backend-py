from functools import lru_cache
from typing import Optional

from sqlmodel import Field, Session, SQLModel, select

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


class DbVertex(SQLModel, table=True):
    """顶点 (没有属性)"""

    class Meta:
        tablename: str = "vertices"

    vid: str = Field(primary_key=True)
    label: str = Field(index=True)

    def __init__(self, vid: str, label: str, attrs: Optional[AttrDict] = None) -> None:
        super().__init__(vid=vid, label=label)
        self._pending_attrs: AttrDict = attrs or {}

    def load_pending_attrs(self, session: Session):
        """加载待添加的属性到数据库中"""

        attributes: list[VertexAttribute] = []
        for key, value in self._pending_attrs.items():
            attr = VertexAttribute(
                vid=self.vid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)
            attributes.append(attr)

        self._pending_attrs.clear()  # 清除待添加的属性
        self.get_attribute.cache_clear()  # 清除缓存
        return attributes

    @lru_cache
    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(VertexAttribute).where(
            (VertexAttribute.vid == self.vid) & (VertexAttribute.key == key)
        )
        attr = session.exec(stmt).first()
        return attr.typed_value if attr else None


class DbEdge(SQLModel, table=True):
    """边 (没有属性)"""

    class Meta:
        tablename: str = "edges"

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

    def load_pending_attrs(self, session: Session):
        """加载待添加的属性到数据库中"""

        attributes: list[EdgeAttribute] = []

        for key, value in self._pending_attrs.items():
            attr = EdgeAttribute(
                eid=self.eid, key=key, value=str(value), type=type(value).__name__
            )
            session.add(attr)
            attributes.append(attr)

        self._pending_attrs.clear()  # 清除待添加的属性
        self.get_attribute.cache_clear()  # 清除缓存
        return attributes

    @lru_cache
    def get_attribute(self, session: Session, key: str) -> Optional[Attr]:
        """获取属性值"""

        stmt = select(EdgeAttribute).where(
            (EdgeAttribute.eid == self.eid) & (EdgeAttribute.key == key)
        )
        attr = session.exec(stmt).first()
        return attr.typed_value if attr else None


class VertexAttribute(BaseAttribute, table=True):
    """顶点属性"""

    class Meta:
        tablename: str = "vertex_attributes"

    vid: str = Field(index=True)


class EdgeAttribute(BaseAttribute, table=True):
    """边属性"""

    class Meta:
        tablename: str = "edge_attributes"

    eid: str = Field(index=True)
