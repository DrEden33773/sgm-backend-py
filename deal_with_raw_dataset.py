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

    src_v_label = file_path.stem.split("_")[0].capitalize()
    e_label = file_path.stem.split("_")[1]
    dst_v_label = file_path.stem.split("_")[2].capitalize()

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f":START_ID({src_v_label})"
    if len(columns) > 1 and "(" not in columns[1] and ")" not in columns[1]:
        rename_mapping[columns[1]] = f":END_ID({dst_v_label})"

    # 重命名列
    if rename_mapping:
        df = df.rename(rename_mapping)
        updated = True
    # 插入新列 (:TYPE)
    if ":TYPE" not in df.columns:
        df = df.with_columns(pl.lit(e_label).alias(":TYPE"))
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

    v_label = file_path.stem.split("_")[0].capitalize()

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and columns[0] == "id":
        # 先指定一个 `没有存入属性 (properties/attributes)` 的 `id` (以 `string` 类型存储)
        rename_mapping[columns[0]] = f":ID({v_label})"
        # 再插入一列 `attr_id` (指定 `long` 类型存储)
        df = df.with_columns(pl.col(columns[0]).cast(pl.Utf8).alias("attr_id:long"))

    # 重命名列
    if rename_mapping:
        df = df.rename(rename_mapping)
        # 重命名, 这就是 `属性` 字段了
        df = df.rename({"attr_id:long": "id:long"})
        updated = True

    if "type" in df.columns:
        # 删除原始 type 列, 并将其转换为 :LABEL 列 (每个元素都 capitalize)
        df = df.with_columns(
            pl.col("type").cast(pl.Utf8).str.to_titlecase().alias(":LABEL")
        ).drop("type")
        updated = True
    elif ":LABEL" not in df.columns:
        # 插入新列 (:LABEL)
        df = df.with_columns(pl.lit(v_label).alias(":LABEL"))
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
