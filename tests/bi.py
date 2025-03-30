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


def test_bi_2_on_sf01():
    plan_name = "ldbc-bi-2.json"
    exec(plan_name)


def test_bi_3_on_sf01():
    plan_name = "ldbc-bi-3.json"
    exec(plan_name)


def test_bi_5_on_sf01():
    plan_name = "ldbc-bi-5.json"
    exec(plan_name)


def test_bi_6_on_sf01():
    plan_name = "ldbc-bi-6.json"
    exec(plan_name)


def test_bi_10_on_sf01():
    plan_name = "ldbc-bi-10.json"
    exec(plan_name)


def test_bi_11_on_sf01():
    plan_name = "ldbc-bi-11.json"
    exec(plan_name)


def test_bi_14_on_sf01():
    plan_name = "ldbc-bi-14.json"
    exec(plan_name)
