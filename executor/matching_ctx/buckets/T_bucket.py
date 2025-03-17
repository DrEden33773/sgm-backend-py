from dataclasses import dataclass, field

from executor.matching_ctx.buckets.A_bucket import A_Bucket
from executor.matching_ctx.type_aliases import PgVid
from utils.expanding_graph import ExpandGraph


@dataclass
class T_Bucket:
    """
    临时交集 (T) 桶

    - 实际上非常像 `A` 桶
        - 准确的说, 是 `A` 桶的一个子集
    """

    target_pat_vid: PgVid

    expanding_graphs: list[ExpandGraph] = field(default_factory=list)

    @classmethod
    def build_from_A_A(cls, left: A_Bucket, right: A_Bucket, target_pat_vid: PgVid):
        left_group = left.dst_pat_grouped_expanding.pop(target_pat_vid, [])
        right_group = right.dst_pat_grouped_expanding.pop(target_pat_vid, [])
        expanding_graphs = cls.expand_edges_on_same_vertices(left_group, right_group)
        return cls(target_pat_vid, expanding_graphs)

    @classmethod
    def build_from_T_A(cls, left: "T_Bucket", right: A_Bucket):
        left_group = left.expanding_graphs
        right_group = right.dst_pat_grouped_expanding.pop(left.target_pat_vid, [])

        # 事实上, 这里可以肯定地说, `T_Bucket` 就是不完整的, 而 `A_Bucket` 就是未被利用的
        # 所以显式指定参数位置
        expanding_graphs = cls.expand_edges_on_same_vertices(
            potential_incomplete_group=left_group, potential_unused_group=right_group
        )
        return cls(left.target_pat_vid, expanding_graphs)

    @staticmethod
    def expand_edges_on_same_vertices(
        potential_unused_group: list[ExpandGraph],
        potential_incomplete_group: list[ExpandGraph],
    ):
        """
        ## 在共同点上, 逐位置扩张边

        ### 目的

        把 `未被利用的` 垂悬边, 尝试连接在 `更年轻` 的图上

        ### 说明

        更短的列表中的图, 经由 `Foreach` 迭代的次数 `更少`

        不难发现: `Foreach` 的迭代次数, 正比于, 对应 `依赖变量` 的年龄

        也就说明, 其中未被 `利用 (连接)` 的 `垂悬边` 存在的概率更大
        """

        result: list[ExpandGraph] = []

        outer_, inner_ = potential_unused_group, potential_incomplete_group
        if len(outer_) > len(inner_):
            outer_, inner_ = inner_, outer_

        for outer in outer_:
            for inner in inner_:
                if not (outer.get_vid_set() & inner.get_vid_set()):
                    # 先在 `点` 上取交集
                    continue

                # 虽然理论上来说 outer 是 unused 概率更大, 但是还是要严格的判断
                # 判断逻辑: 谁是子集, 谁就是 unused
                unused, incomplete = outer, inner
                if not outer.get_vid_set() <= inner.get_vid_set():
                    unused, incomplete = inner, outer

                # 再对共同点的 `边`, 取并集, 进行扩张
                # 注意, 这里会按照 `unused` 边的数量, 拷贝多份 `incomplete` 图
                # 每份 incomplete 图, 都会连接一条 unused 边
                unions = ExpandGraph.intersect_then_union_on_same_v(unused, incomplete)
                result.extend(unions)

        return result
