from pathlib import Path

import polars as pl

ROOT = Path(__file__).parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes" / "all"
RELATIONSHIPS = TEST_DATASET / "relationships" / "all"


def transform_relationship_csv(file_path: Path):
    updated = False
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    _0th = file_path.stem.split("_")[0].capitalize()
    _label = file_path.stem.split("_")[1]
    _2th = file_path.stem.split("_")[2].capitalize()

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f":START_ID({_0th})"
    if len(columns) > 1 and "(" not in columns[1] and ")" not in columns[1]:
        rename_mapping[columns[1]] = f":END_ID({_2th})"

    # 重命名列
    if rename_mapping:
        df = df.rename(rename_mapping)
        updated = True
    # 插入新列 (:TYPE)
    if ":TYPE" not in df.columns:
        df = df.with_columns(pl.lit(_label).alias(":TYPE"))
        updated = True

    if updated:
        df.write_csv(file_path, separator="|")
        print(f"成功更新文件 {file_path}.")
    else:
        print(f"文件 {file_path} 无需更新.")


def transform_node_csv(file_path: Path):
    updated = False
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    _0th = file_path.stem.split("_")[0].capitalize()
    _label = _0th

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f"id:ID({_0th})"

    # 重命名列
    if rename_mapping:
        df = df.rename(rename_mapping)
        updated = True
    # 插入新列 (:LABEL)
    if ":LABEL" not in df.columns:
        df = df.with_columns(pl.lit(_label).alias(":LABEL"))
        updated = True

    if updated:
        df.write_csv(file_path, separator="|")
        print(f"成功更新文件 {file_path}.")
    else:
        print(f"文件 {file_path} 无需更新.")


if __name__ == "__main__":
    for node_file in NODES.iterdir():
        if node_file.suffix == ".csv":
            transform_node_csv(node_file)

    for relationship_file in RELATIONSHIPS.iterdir():
        if relationship_file.suffix == ".csv":
            transform_relationship_csv(relationship_file)
