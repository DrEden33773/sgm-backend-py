from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import override


class CandidateSet(ABC):
    @abstractmethod
    def __and__[T: "CandidateSet"](self, other: T) -> T:
        pass


if __name__ == "__main__":

    @dataclass
    class SetLike(CandidateSet):
        data: set[int] = field(default_factory=set)

        @override
        def __and__(self, other: "SetLike") -> "SetLike":
            return SetLike(self.data & other.data)

    @dataclass
    class DictLike(CandidateSet):
        data: dict[int, int] = field(default_factory=dict)

        @override
        def __and__(self, other: "DictLike") -> "DictLike":
            return DictLike({k: v for k, v in self.data.items() if k in other.data})

    candidate_sets: list[CandidateSet] = [
        SetLike({1, 2, 3}),
        DictLike({1: 1, 2: 2, 3: 3}),
    ]

    s0 = candidate_sets[0] & candidate_sets[0]  # OK
    s1 = (
        candidate_sets[0] & candidate_sets[1]
    )  # TypeError: unsupported operand type(s) for &: 'set' and 'dict'
