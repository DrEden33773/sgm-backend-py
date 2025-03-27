import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.absolute()
BI_DATA_DIR = ROOT / "data" / "ldbc-sn-bi-sf1"
NODES = BI_DATA_DIR / "nodes"
RELATIONSHIPS = BI_DATA_DIR / "relationships"

NEO4J_IMPORT_PRE = "neo4j-admin database import full"
NEO4J_IMPORT_OPT = '--delimiter="|" --threads=20 --high-parallel-io=on --overwrite-destination --verbose'
NEO4J_IMPORT = f"{NEO4J_IMPORT_PRE} {NEO4J_IMPORT_OPT}"

if __name__ == "__main__":
    cmd_partials = [NEO4J_IMPORT]

    for node_folder in NODES.iterdir():
        if not node_folder.is_dir():
            continue

        for node_file in node_folder.iterdir():
            partial = f'--nodes="{node_file}"'
            cmd_partials.append(partial)

    for relationship_folder in RELATIONSHIPS.iterdir():
        if not relationship_folder.is_dir():
            continue

        for relationship_file in relationship_folder.iterdir():
            partial = f'--relationships="{relationship_file}"'
            cmd_partials.append(partial)

    subprocess.call(" ".join(cmd_partials), shell=True)
