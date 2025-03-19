from pathlib import Path

from executor import ExecEngine
from sqlite_dg_builder.more_triangles import MoreTriangleDgBuilder
from sqlite_dg_builder.triangles import TriangleDgBuilder
from utils.dbg import pprint

SCRIPT_DIR = Path(__file__).parent.absolute()
FILEPATH = SCRIPT_DIR / "plan.json"


def test_triangle_plan():
    TriangleDgBuilder().build()
    result = ExecEngine.from_json(FILEPATH.read_text()).exec_without_final_join()
    print("\nResult:")
    pprint(result)


def test_more_triangle_plan():
    MoreTriangleDgBuilder().build()
    result = ExecEngine.from_json(FILEPATH.read_text()).exec_without_final_join()
    print("\nResult:")
    pprint(result)


if __name__ == "__main__":
    test_more_triangle_plan()
