from pathlib import Path

from config import LDBC_SNB_INTERACTIVE_DB_URL
from executor import ExecEngine
from sqlite_dg_builder.ic_6 import IC6Builder
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from storage.sqlite import SQLiteStorageAdapter
from utils.dbg import pprint
from utils.tracked_lru_cache import clear_all_tracked_caches

SCRIPT_DIR = Path(__file__).parent.absolute()
PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def test_triangle_forest():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json((PLAN_DIR / "forest.json").read_text()).exec()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_more_triangle_forest():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json((PLAN_DIR / "forest.json").read_text()).exec()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_ic_6_simplified():
    IC6Builder().build()
    result = ExecEngine.from_json(
        (PLAN_DIR / "ldbc-ic-6-simplified.json").read_text()
    ).exec()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_ic_6():
    IC6Builder().build()
    result = ExecEngine.from_json((PLAN_DIR / "ldbc-ic-6.json").read_text()).exec()

    print("\nResult:")
    pprint(result)
    clear_all_tracked_caches()


def test_ic_6_on_sf01():
    plan_name = "ldbc-ic-6-single-directed-knows.json"
    result = ExecEngine.from_json(
        (PLAN_DIR / plan_name).read_text(),
        storage_adapter=SQLiteStorageAdapter(db_url=LDBC_SNB_INTERACTIVE_DB_URL),
    ).exec()

    print(f"\nCOUNT(result) = {len(result)}")
    print("\nresult:")
    ExecEngine.project_all_ids(result)
    clear_all_tracked_caches()


def test_ic_1_on_sf01():
    plan_name = "ldbc-ic-1-single-directed-knows.json"
    result = ExecEngine.from_json(
        (PLAN_DIR / plan_name).read_text(),
        storage_adapter=SQLiteStorageAdapter(db_url=LDBC_SNB_INTERACTIVE_DB_URL),
    ).exec()

    print(f"\nCOUNT(result) = {len(result)}")
    print("\nresult:")
    ExecEngine.project_all_ids(result)
    clear_all_tracked_caches()


if __name__ == "__main__":
    # test_triangle_forest()
    # test_more_triangle_forest()
    # test_ic_6_simplified()
    # test_ic_6()

    test_ic_6_on_sf01()
    # test_ic_1_on_sf01()

    pass
