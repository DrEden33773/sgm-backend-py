from typing import Optional

from sqlmodel import Field, SQLModel, create_engine

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
