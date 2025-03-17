from dataclasses import dataclass, field

from executor.matching_ctx.buckets.C_bucket import C_Bucket
from utils.dyn_graph import DynGraph


@dataclass
class F_Bucket:
    """枚举目标 (f) 桶"""

    all_matched: list[DynGraph] = field(default_factory=list)

    def __post_init__(self):
        self.append_matched = self.all_matched.append

    @classmethod
    def from_C_bucket(cls, C_bucket: C_Bucket):
        all_matched: list[DynGraph] = []

        # 无需 `分裂式` 枚举, 直接导出
        if not C_bucket.need_further_enumeration:
            all_matched = [g.to_dyn_graph_emplace() for g in C_bucket.all_expanded]
            return cls(all_matched)

        # 需要 `分裂式` 枚举
        for expanded in C_bucket.all_expanded:
            target_v = expanded.target_v_entities.values()
            for v in target_v:
                # 这里得用 deepcopy (cloned), 因为每个 target_v 都会生成一张图
                new_dg = expanded.to_dyn_graph_cloned()
                new_dg.update_v(v)
                all_matched.append(new_dg)

        return cls(all_matched)
