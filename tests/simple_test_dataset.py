from config import SCRIPT_DIR
from executor import ExecEngine
from sqlite_dg_builder.bi_6 import BI6Builder
from sqlite_dg_builder.ic_4 import IC4Builder
from sqlite_dg_builder.ic_5 import IC5Builder
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from storage.sqlite import SQLiteStorageAdapter
from utils.tracked_lru_cache import clear_all_tracked_caches

PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def test_triangle_forest():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "forest.json").read_text(), SQLiteStorageAdapter()
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_more_triangle_forest():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "forest.json").read_text(), SQLiteStorageAdapter()
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_minimized_ic_4():
    IC4Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-4-single-directed-knows.json").read_text(),
        SQLiteStorageAdapter(),
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_minimized_ic_5():
    IC5Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-5-single-directed-knows.json").read_text(),
        SQLiteStorageAdapter(),
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_minimized_bi_6():
    BI6Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-bi-6.json").read_text(), SQLiteStorageAdapter()
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()
