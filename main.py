from pathlib import Path

from executor import ExecEngine
from sqlite_dg_builder.ic_6 import IC6Builder
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from utils.dbg import pprint
from utils.tracked_lru_cache import clear_all_tracked_caches

SCRIPT_DIR = Path(__file__).parent.absolute()
PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def test_triangle_forest():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "forest.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_more_triangle_forest():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "forest.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_ldbc_ic_6():
    IC6Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-6.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_ldbc_ic_6_simplified():
    IC6Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-6-simplified.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


if __name__ == "__main__":
    # test_triangle_forest()
    # test_more_triangle_forest()
    # test_ldbc_ic_6_simplified()
    test_ldbc_ic_6()
    pass
