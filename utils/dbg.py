import pprint as pp
from functools import lru_cache

from schema import Instruction

WIDTH = 40


@lru_cache
def header(header: str, splitter: str = "=", rep_times: int = WIDTH // 2) -> str:
    return f"{splitter * rep_times} {header} {splitter * rep_times}"


def print_header(str: str, splitter: str = "=", rep_times: int = WIDTH // 2) -> None:
    print(header(str, splitter, rep_times))


def pprint(obj: object):
    pp.pprint(obj, indent=2, width=WIDTH, sort_dicts=False, compact=False)


def pprint_instr(instr: Instruction):
    print_header("↓↓↓")
    pprint(instr)
    print_header("↑↑↑")
