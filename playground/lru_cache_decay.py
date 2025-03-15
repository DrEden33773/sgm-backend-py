if __name__ == "__main__":
    from functools import lru_cache

    data = {"a": 1, "b": 2, "c": 3}

    @lru_cache
    def get_data(key: str):
        return data.get(key)

    print(get_data("a"))
    print(get_data("b"))
    print(get_data("c"))
    data.pop("a")
    get_data.cache_clear()  # 必须清除缓存
    print(get_data("a"))
