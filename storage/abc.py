from abc import ABC, abstractmethod
from typing import Optional

from schema import Attr, DataEdge, DataVertex, Label


class StorageAdapter(ABC):
    """`存储适配器` 抽象基类"""

    @abstractmethod
    def load_vertices(
        self,
        v_label: Label,
        v_attr: Optional[Attr] = None,
    ) -> list[DataVertex]:
        """
        ## Init

        根据 `label` 和 `attr` 加载顶点
        """

    @abstractmethod
    def load_edges(
        self,
        e_label: Label,
        e_attr: Optional[Attr] = None,
    ) -> list[DataEdge]:
        """
        ## Init

        根据 `label` 和 `attr` 加载边
        """
