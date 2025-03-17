if __name__ == "__main__":

    def append_element_in[T](e: T, list: list[T]):
        list.append(e)

    list_ = [1, 2, 3]
    append_element_in(4, list_)
    print(list_)
    assert list_ == [1, 2, 3, 4]
