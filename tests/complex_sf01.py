from config import LDBC_SNB_INTERACTIVE_SQL_DB_URL, SCRIPT_DIR, WHICH_DB
from executor import ExecEngine
from storage.neo4j import Neo4jStorageAdapter
from storage.sqlite import SQLiteStorageAdapter
from utils.tracked_lru_cache import clear_all_tracked_caches

PLAN_DIR = SCRIPT_DIR / "resources" / "plan"


def exec(plan_name: str):
    result = ExecEngine.from_json(
        (PLAN_DIR / plan_name).read_text(),
        storage_adapter=SQLiteStorageAdapter(db_url=LDBC_SNB_INTERACTIVE_SQL_DB_URL)
        if WHICH_DB == "SQLite"
        else Neo4jStorageAdapter(),
    ).exec()

    ExecEngine.project_all_ids(result)
    print(f"\nCOUNT(result) = {len(result)}\n")
    clear_all_tracked_caches()


def test_ic_6_on_sf01():
    plan_name = "ldbc-ic-6-single-directed-knows.json"
    exec(plan_name)


def test_ic_1_on_sf01():
    plan_name = "ldbc-ic-1-single-directed-knows.json"
    exec(plan_name)


def test_ic_11_on_sf01():
    plan_name = "ldbc-ic-11-single-directed-knows.json"
    exec(plan_name)


def test_ic_12_on_sf01():
    plan_name = "ldbc-ic-12-single-directed-knows.json"
    exec(plan_name)


def test_ic_4_on_sf01():
    plan_name = "ldbc-ic-4-single-directed-knows.json"
    exec(plan_name)


def test_ic_5_on_sf01():
    plan_name = "ldbc-ic-5-single-directed-knows.json"
    exec(plan_name)
