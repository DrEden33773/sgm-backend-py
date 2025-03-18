from pathlib import Path

from executor import ExecEngine
from utils.dbg import pprint

SCRIPT_DIR = Path(__file__).parent.absolute()
plan_json_filepath = SCRIPT_DIR / "plan.json"

result = ExecEngine.from_json(plan_json_filepath.read_text()).exec_without_final_join()
print("\nResult:")
pprint(result)
