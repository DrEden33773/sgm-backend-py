from dataclasses import dataclass, field

if __name__ == "__main__":

    @dataclass
    class WrappedList[T]:
        data: list[T] = field(default_factory=list)

        def __post_init__(self):
            self.append = self.data.append
            self.extend = self.data.extend

        def __iter__(self):
            return iter(self.data)

        def __contains__(self, item: T) -> bool:
            return item in self.data

        def __repr__(self) -> str:
            return repr(self.data)

    wl = WrappedList[int]()
    wl.append(1)
    wl.append(2)
    wl.append(3)
    wl.extend([4, 5, 6])
    print(wl)
