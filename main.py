from pathlib import Path

from executor import ExecEngine
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from utils.dbg import pprint
from utils.tracked_lru_cache import clear_all_tracked_caches

SCRIPT_DIR = Path(__file__).parent.absolute()
PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def test_triangle_plan():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "triangle.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)

    clear_all_tracked_caches()


def test_more_triangle_plan():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "triangle.json").read_text()
    ).exec_without_final_join()

    print("\nResult:")
    pprint(result)

    clear_all_tracked_caches()


if __name__ == "__main__":
    test_triangle_plan()
    test_more_triangle_plan()
