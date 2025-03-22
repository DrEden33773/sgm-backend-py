from pathlib import Path
from typing import Dict

import polars as pl

ROOT = Path(__file__).parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes" / "all"
RELATIONSHIPS = TEST_DATASET / "relationships" / "all"


def transform_relationship_csv(file_path: Path):
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    _0th = file_path.stem.split("_")[0].capitalize()
    _2th = file_path.stem.split("_")[2].capitalize()

    rename_mapping: Dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f":START_ID({_0th})"
    if len(columns) > 1 and "(" not in columns[1] and ")" not in columns[1]:
        rename_mapping[columns[1]] = f":END_ID({_2th})"

    if rename_mapping:
        df = df.rename(rename_mapping)
        df.write_csv(file_path, separator="|")
        print(f"成功更新文件 {file_path} 的列名.")
    else:
        print(f"文件 {file_path} 没有需要修改的列.")


def transform_node_csv(file_path: Path):
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    _0th = file_path.stem.split("_")[0].capitalize()

    rename_mapping: Dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f"id:ID({_0th})"

    if rename_mapping:
        df = df.rename(rename_mapping)
        df.write_csv(file_path, separator="|")
        print(f"成功更新文件 {file_path} 的列名.")
    else:
        print(f"文件 {file_path} 没有需要修改的列.")


if __name__ == "__main__":
    for node_file in NODES.iterdir():
        if node_file.suffix == ".csv":
            transform_node_csv(node_file)

    for relationship_file in RELATIONSHIPS.iterdir():
        if relationship_file.suffix == ".csv":
            transform_relationship_csv(relationship_file)
