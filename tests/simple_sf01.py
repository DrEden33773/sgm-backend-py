from config import LDBC_SNB_INTERACTIVE_DB_URL, SCRIPT_DIR
from executor import ExecEngine
from storage.sqlite import SQLiteStorageAdapter
from utils.tracked_lru_cache import clear_all_tracked_caches

PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def exec(plan_name: str):
    result = ExecEngine.from_json(
        (PLAN_DIR / plan_name).read_text(),
        storage_adapter=SQLiteStorageAdapter(db_url=LDBC_SNB_INTERACTIVE_DB_URL),
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_is_1_on_sf01():
    plan_name = "ldbc-is-1.json"
    exec(plan_name)


def test_is_3_on_sf01():
    plan_name = "ldbc-is-3-single-directed-knows.json"
    exec(plan_name)


def test_is_3_double_directed_knows_on_sf01():
    plan_name = "ldbc-is-3-double-directed-knows.json"
    exec(plan_name)


def test_is_3_reversed_directed_knows_on_sf01():
    plan_name = "ldbc-is-3-reversed-directed-knows.json"
    exec(plan_name)
