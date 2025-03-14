import pprint as pp
from functools import lru_cache


@lru_cache
def header(header: str, splitter: str = "=", rep_times: int = 30) -> str:
    return f"{splitter * rep_times} {header} {splitter * rep_times}"


def print_header(str: str, splitter: str = "=", rep_times: int = 30) -> None:
    print(header(str, splitter, rep_times))


def pprint(obj: object):
    pp.pprint(obj, indent=2, width=60, sort_dicts=False)
