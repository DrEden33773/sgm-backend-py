from dataclasses import dataclass, field

if __name__ == "__main__":

    @dataclass
    class Wrapper:
        list_: list[int] = field(default_factory=list)
        size: int = 0

        def __post_init__(self):
            self.size = len(self.list_)

    list_a = [1, 2, 3]
    wrapper = Wrapper(list_a)
    print(wrapper.size)
