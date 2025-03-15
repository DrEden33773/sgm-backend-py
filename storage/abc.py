from abc import ABC, abstractmethod
from typing import Optional

from schema import Attr, Edge, Label, Vertex


class StorageAdapter(ABC):
    """`存储适配器` 抽象基类"""

    @abstractmethod
    async def load_vertices(
        self,
        label: Label,
        attr: Optional[Attr] = None,
    ) -> list[Vertex]:
        """
        ## Init

        根据 `label` 和 `attr` 加载顶点
        """

    @abstractmethod
    async def expand_edges_of(
        self,
        src_v_label: Label,
        e_label: Label,
        dst_v_label: Label,
        src_v_attr: Optional[Attr] = None,
        e_attr: Optional[Attr] = None,
        dst_v_attr: Optional[Attr] = None,
    ) -> list[Edge]:
        """
        ## GetAdj

        对数据图上符合 `^[条件一]` 的顶点, 加载符合 `^[条件二]` 的 `expand_edges`

        - 条件一:
            - 源顶点标签 `src_v_label`
                - 源顶点属性 `src_v_attr` (可选)

        - 条件二:
            - 边标签 `e_label`
            - 目标顶点标签 `dst_v_label`
                - 边属性 `e_attr` (可选)
                - 目标顶点属性 `dst_v_attr` (可选)
        """
