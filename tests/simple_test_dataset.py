from config import SCRIPT_DIR
from executor import ExecEngine
from sqlite_dg_builder.ic_4 import IC4Builder
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from utils.tracked_lru_cache import clear_all_tracked_caches

PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def test_triangle_forest():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json((PLAN_DIR / "forest.json").read_text()).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_more_triangle_forest():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json((PLAN_DIR / "forest.json").read_text()).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_minimized_ic_4():
    IC4Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-4-single-directed-knows.json").read_text()
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()
