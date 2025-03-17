if __name__ == "__main__":
    from enum import StrEnum

    class Wrapper(StrEnum):
        A = "a"
        B = "b"
        C = "c"

    a = Wrapper("a")
    b = Wrapper("b")
    c = Wrapper("c")

    if "d" not in Wrapper:
        print("d not in Wrapper")
    if "a" in Wrapper:
        print("a in Wrapper")
