"""
这个模块提供SQLModel到SQLAlchemy Core的兼容性转换
"""

from typing import Any, Optional


class DB_Vertex:
    """兼容性顶点类"""

    def __init__(
        self, vid: str, label: str, attrs: Optional[dict[str, Any]] = None
    ) -> None:
        self.vid = vid
        self.label = label
        self._pending_attrs = attrs or {}

    def get_attributes(self) -> dict[str, Any]:
        """获取属性 (兼容性方法)"""
        return self._pending_attrs

    def load_pending_attrs(self):
        """加载待添加的属性 (兼容性方法)"""
        self._pending_attrs = {}

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return {
            "vid": self.vid,
            "label": self.label,
            "_pending_attrs": self._pending_attrs,
        }


class DB_Edge:
    """兼容性边类"""

    def __init__(
        self,
        eid: str,
        src_vid: str,
        dst_vid: str,
        label: str,
        attrs: Optional[dict[str, Any]] = None,
    ) -> None:
        self.eid = eid
        self.src_vid = src_vid
        self.dst_vid = dst_vid
        self.label = label
        self._pending_attrs = attrs or {}

    def get_attributes(self) -> dict[str, Any]:
        """获取属性 (兼容性方法)"""
        return self._pending_attrs

    def load_pending_attrs(self):
        """加载待添加的属性 (兼容性方法)"""
        self._pending_attrs = {}

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        return {
            "eid": self.eid,
            "label": self.label,
            "src_vid": self.src_vid,
            "dst_vid": self.dst_vid,
            "_pending_attrs": self._pending_attrs,
        }
