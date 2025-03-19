from abc import ABC, abstractmethod
from functools import lru_cache

from schema import DataEdge, DataVertex, Label, PatternAttr


class StorageAdapter(ABC):
    """`存储适配器` 抽象基类"""

    @abstractmethod
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        """
        ## Init

        根据 `label` 加载顶点
        """

    @abstractmethod
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        """
        ## Init

        根据 `label` 和 `attr` 加载顶点
        """

    @lru_cache
    @abstractmethod
    def load_e(self, e_label: Label) -> list[DataEdge]:
        """
        ## Init

        根据 `label` 加载边
        """

    @lru_cache
    @abstractmethod
    def load_e_with_attr(
        self,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        """
        ## Init

        根据 `label` 和 `attr` 加载边
        """
