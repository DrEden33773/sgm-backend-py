import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes" / "all"
RELATIONSHIPS = TEST_DATASET / "relationships" / "all"

NEO4J_IMPORT_PRE = "neo4j-admin database import full"
NEO4J_IMPORT_OPT = '--delimiter="|" --threads=20 --high-parallel-io=on --overwrite-destination --verbose'
NEO4J_IMPORT = f"{NEO4J_IMPORT_PRE} {NEO4J_IMPORT_OPT}"

if __name__ == "__main__":
    cmd_partials = [NEO4J_IMPORT]

    for node_file in NODES.iterdir():
        partial = f'--nodes="{node_file}"'
        cmd_partials.append(partial)

    for relationship_file in RELATIONSHIPS.iterdir():
        partial = f'--relationships="{relationship_file}"'
        cmd_partials.append(partial)

    subprocess.call(" ".join(cmd_partials), shell=True)
