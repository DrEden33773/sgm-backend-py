from abc import ABC, abstractmethod
from functools import lru_cache

from schema import DataEdge, DataVertex, Label, PatternAttr, Vid
from utils.tracked_lru_cache import track_lru_cache_annotated


class StorageAdapter(ABC):
    """`存储适配器` 抽象基类"""

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def get_v(self, vid: Vid) -> DataVertex:
        """
        ## Init

        根据 `vid` 找到顶点
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        """
        ## Init

        根据 `label` 加载顶点
        """

    @abstractmethod
    @track_lru_cache_annotated
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

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e(self, e_label: Label) -> list[DataEdge]:
        """
        ## Init

        根据 `label` 加载边
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid(self, src_vid: Vid, e_label: Label) -> list[DataEdge]:
        """
        ## Init

        根据 `src_vid` 和 `label` 加载边
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid(self, dst_vid: Vid, e_label: Label) -> list[DataEdge]:
        """
        ## Init

        根据 `dst_vid` 和 `label` 加载边
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e_with_attr(
        self,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        """
        ## Init

        根据 `label` 和 `attr` 加载边
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_src_vid_with_attr(
        self,
        src_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        """
        ## Init

        根据 `src_vid`, `label` 和 `attr` 加载边
        """

    @abstractmethod
    @track_lru_cache_annotated
    @lru_cache
    def load_e_by_dst_vid_with_attr(
        self,
        dst_vid: Vid,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        """
        ## Init

        根据 `dst_vid`, `label` 和 `attr` 加载边
        """
