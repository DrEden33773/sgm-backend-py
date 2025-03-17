from dataclasses import dataclass, field

from executor.matching_ctx.buckets.A_bucket import A_Bucket
from executor.matching_ctx.buckets.T_bucket import T_Bucket
from executor.matching_ctx.type_aliases import DgVertex, PgVid
from utils.expanding_graph import ExpandGraph


@dataclass
class C_Bucket:
    """候选集 (C) 桶"""

    all_expanded: list[ExpandGraph] = field(default_factory=list)
    need_further_enumeration: bool = True

    @classmethod
    def build_from_A(
        cls, A_bucket: A_Bucket, curr_pat_vid: PgVid, loaded_vertices: list[DgVertex]
    ):
        # 从 A_bucket 中弹出当前分组
        curr_group = A_bucket.dst_pat_grouped_expanding.pop(curr_pat_vid, [])
        if not curr_group:
            return cls()

        all_expanded: list[ExpandGraph] = []

        for expanding in curr_group:
            # 与给定点集 `loaded_vertices` 求交
            expanding.update_available_targets(loaded_vertices)

            # 更新 `all_expanded`
            all_expanded.append(expanding)

        # 从 `A_bucket` 构造的图, 后续还需要 `进一步枚举`
        return cls(all_expanded, need_further_enumeration=True)

    @classmethod
    def build_from_T(cls, T_bucket: T_Bucket, loaded_vertices: list[DgVertex]):
        if not T_bucket.expanding_graphs:
            return cls()

        all_expanded: list[ExpandGraph] = []

        for expanding in T_bucket.expanding_graphs:
            # 与给定点集 `loaded_vertices` 求交
            expanding.update_available_targets(loaded_vertices)

            # 更新 `all_expanded`
            all_expanded.append(expanding)

        # 从 `T_bucket` 构造的图, 已经被 `完全枚举`
        return cls(all_expanded, need_further_enumeration=False)
