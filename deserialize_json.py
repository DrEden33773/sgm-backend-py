from pathlib import Path

from executor import ExecEngine

SCRIPT_DIR = Path(__file__).parent.absolute()
plan_json_filepath = SCRIPT_DIR / "plan.json"
ExecEngine.deserialize_json(plan_json_filepath.read_text())
