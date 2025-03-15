from abc import ABC, abstractmethod
from typing import Optional

from schema import Attr, Edge, Label, Vertex, Vid


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
        dg_src_vid: Vid,
        e_label: Label,
        dst_v_label: Label,
        e_attr: Optional[Attr] = None,
        dst_v_attr: Optional[Attr] = None,
    ) -> list[Edge]:
        """
        ## GetAdj

        对数据图上的顶点 `dg_src_vid`, 加载符合条件的 `expand_edges`

        - 条件:
            - 边标签 `e_label`
            - 目标顶点标签 `dst_v_label`
                - 边属性 `e_attr` (可选)
                - 目标顶点属性 `dst_v_attr` (可选)
        """
