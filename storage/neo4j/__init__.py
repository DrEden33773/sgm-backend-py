from functools import lru_cache
from typing import override

from schema import DataEdge, DataVertex, Label, PatternAttr
from storage.abc import StorageAdapter


class Neo4jStorageAdapter(StorageAdapter):
    """Neo4j 存储适配器"""

    @override
    @lru_cache
    def get_v(self, vid: str) -> DataVertex:
        raise NotImplementedError

    @override
    @lru_cache
    def load_v(self, v_label: Label) -> list[DataVertex]:
        raise NotImplementedError

    @override
    @lru_cache
    def load_v_with_attr(
        self,
        v_label: Label,
        v_attr: PatternAttr,
    ) -> list[DataVertex]:
        raise NotImplementedError

    @override
    @lru_cache
    def load_e(self, e_label: Label) -> list[DataEdge]:
        raise NotImplementedError

    @override
    @lru_cache
    def load_e_with_attr(
        self,
        e_label: Label,
        e_attr: PatternAttr,
    ) -> list[DataEdge]:
        raise NotImplementedError
