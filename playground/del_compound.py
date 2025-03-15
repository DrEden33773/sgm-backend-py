from dataclasses import dataclass, field

if __name__ == "__main__":

    @dataclass
    class Wrapper:
        dict_: dict[str, int] = field(default_factory=dict)
        set_: set[int] = field(default_factory=set)
        list_: list[int] = field(default_factory=list)

    dict_a = {"a": 1, "b": 2, "c": 3}
    set_a = {1, 2, 3}
    list_a = [1, 2, 3]

    wrapper = Wrapper(dict_a, set_a, list_a)
    print(wrapper)
    del wrapper
    print(dict_a, set_a, list_a)
