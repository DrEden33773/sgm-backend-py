from pathlib import Path

import polars as pl

ROOT = Path(__file__).parent.absolute()
BI_DATA_DIR = ROOT / "data" / "ldbc-sn-bi-sf1"
NODES = BI_DATA_DIR / "nodes"
RELATIONSHIPS = BI_DATA_DIR / "relationships"


def deal_with_relationships_in_folder(folder_path: Path):
    SRC_V_LABEL, E_LABEL, DST_V_LABEL = folder_path.stem.split("_")
    RAW_SRC_ID = f"{SRC_V_LABEL}Id"
    RAW_DST_ID = f"{DST_V_LABEL}Id"
    SRC_ID = f":START_ID({SRC_V_LABEL})"
    DST_ID = f":END_ID({DST_V_LABEL})"
    TYPE = ":TYPE"

    for file_path in folder_path.iterdir():
        if file_path.suffix != ".csv":
            # 删除非 CSV 文件
            file_path.unlink()
            continue

        updated = False
        df = pl.read_csv(file_path, separator="|")
        columns = df.columns
        if set((DST_ID, SRC_ID, TYPE)) <= set(df.columns):
            print(f"文件 '{file_path}' 无需更新.")
            continue

        # 插入新列 (:TYPE)
        if TYPE not in columns:
            df = df.with_columns(pl.lit(E_LABEL).alias(TYPE))
            updated = True

        if RAW_SRC_ID not in columns or RAW_DST_ID not in columns:
            print(
                f"警告: 文件 '{file_path}' 中没有 '{RAW_SRC_ID}' 或 '{RAW_DST_ID}' 列"
            )
        else:
            RAW_SRC_ID_IDX = columns.index(RAW_SRC_ID)
            RAW_DST_ID_IDX = columns.index(RAW_DST_ID)
            rename_mapping = {
                columns[RAW_SRC_ID_IDX]: SRC_ID,
                columns[RAW_DST_ID_IDX]: DST_ID,
            }
            # 重命名列
            df = df.rename(rename_mapping)
            updated = True

        if updated:
            df.write_csv(file_path, separator="|")
            print(f"成功更新文件 '{file_path}'.")
        else:
            print(f"文件 '{file_path}' 无需更新.")


def deal_with_nodes_in_folder(folder_path: Path):
    V_LABEL = folder_path.stem
    RAW_ID = "id"
    RAW_TYPE = "type"
    ID = f":ID({V_LABEL})"
    LABEL = ":LABEL"

    for file_path in folder_path.iterdir():
        if file_path.suffix != ".csv":
            # 删除非 CSV 文件
            file_path.unlink()
            continue

        updated = False
        df = pl.read_csv(file_path, separator="|")
        columns = df.columns
        if set((ID, LABEL, "id:long")) <= set(df.columns):
            print(f"文件 '{file_path}' 无需更新.")
            continue

        if RAW_TYPE in df.columns:
            # 删除原始 type 列, 并将其转换为 :LABEL 列 (每个元素都 capitalize)
            df = df.with_columns(
                pl.col(RAW_TYPE).cast(pl.Utf8).str.to_titlecase().alias(LABEL)
            ).drop(RAW_TYPE)
            updated = True
        elif LABEL not in df.columns:
            # 插入新列 (:LABEL)
            df = df.with_columns(pl.lit(V_LABEL).alias(LABEL))
            updated = True

        if RAW_ID not in columns:
            print(f"警告: 文件 '{file_path}' 中没有 'id' 列")
        else:
            RAW_ID_IDX = columns.index(RAW_ID)

            # 先指定一个 `没有存入属性 (properties/attributes)` 的 `id` (以 `string` 类型存储)
            rename_mapping = {columns[RAW_ID_IDX]: ID}
            # 再插入一列 `attr_id` (指定 `long` 类型存储)
            df = df.with_columns(
                pl.col(columns[RAW_ID_IDX]).cast(pl.Utf8).alias("attr_id:long")
            )

            # 重命名列
            df = df.rename(rename_mapping)
            # 重命名, 这就是 `属性` 字段了
            df = df.rename({"attr_id:long": "id:long"})
            updated = True

        if updated:
            df.write_csv(file_path, separator="|")
            print(f"成功更新文件 '{file_path}'.")
        else:
            print(f"文件 '{file_path}' 无需更新.")


if __name__ == "__main__":
    for node_folder in NODES.iterdir():
        if node_folder.is_dir():
            deal_with_nodes_in_folder(node_folder)

    for relationship_folder in RELATIONSHIPS.iterdir():
        if relationship_folder.is_dir():
            deal_with_relationships_in_folder(relationship_folder)
