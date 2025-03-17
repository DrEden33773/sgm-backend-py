from typing import Optional, override

from schema import Attr, Edge, Label, Vertex
from storage.abc import StorageAdapter


class Neo4jStorageAdapter(StorageAdapter):
    @override
    def load_vertices(
        self,
        v_label: Label,
        v_attr: Optional[Attr] = None,
    ) -> list[Vertex]:
        # TODO: Implement this method
        raise NotImplementedError

    @override
    def load_edges(
        self,
        e_label: Label,
        e_attr: Optional[Attr] = None,
    ) -> list[Edge]:
        # TODO: Implement this method
        raise NotImplementedError
