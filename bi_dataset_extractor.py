import glob
import gzip
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, TypeAlias

PathList: TypeAlias = List[Path]
ExtractResult: TypeAlias = Tuple[int, int]  # (成功数量, 总数量)
ROOT = Path(__file__).parent.absolute()


def extract_gzip_file(gzip_file_path: Path) -> bool:
    """
    解压单个 .gz 文件到相同目录（去除 .gz 扩展名）

    Args:
        gzip_file_path: 要解压的 .gz 文件路径

    Returns:
        bool: 解压成功返回 True，否则返回 False
    """
    # 定义输出文件路径（移除 .gz 扩展名）
    output_file_path: Path = gzip_file_path.with_suffix("")

    # 检查输出文件是否已存在
    if output_file_path.exists():
        print(f"警告: 输出文件已存在: {output_file_path}")

    try:
        # 以二进制读取模式打开 gzip 文件
        with gzip.open(gzip_file_path, "rb") as f_in:
            # 以二进制写入模式打开输出文件
            with open(output_file_path, "wb") as f_out:
                # 复制解压后的内容到输出文件
                shutil.copyfileobj(f_in, f_out)

        # 保留原文件的时间戳
        source_stat = os.stat(gzip_file_path)
        os.utime(output_file_path, (source_stat.st_atime, source_stat.st_mtime))

        print(f"成功解压: {gzip_file_path}")
        return True
    except gzip.BadGzipFile:
        print(f"错误: 不是有效的 gzip 文件: {gzip_file_path}")
        return False
    except PermissionError:
        print(f"错误: 访问权限被拒绝: {gzip_file_path}")
        return False
    except Exception as e:
        print(f"解压 {gzip_file_path} 时出错: {str(e)}")
        return False


def find_and_extract_gzip_files(directory_pattern: str) -> ExtractResult:
    """
    查找并解压指定目录模式下的所有 .gz 文件

    Args:
        directory_pattern: 查找 .gz 文件的 glob 模式

    Returns:
        ExtractResult: 包含 (成功数量, 总数量) 的元组
    """
    # 查找所有匹配模式的 .gz 文件
    gzip_files: PathList = [
        Path(p) for p in glob.glob(directory_pattern, recursive=True)
    ]

    if not gzip_files:
        print(f"在 {directory_pattern} 中没有找到 .gz 文件")
        return (0, 0)

    total_count: int = len(gzip_files)
    print(f"在 {directory_pattern} 中找到 {total_count} 个 .gz 文件")

    # 解压每个 .gz 文件
    success_count: int = 0
    for gzip_file in gzip_files:
        if extract_gzip_file(gzip_file):
            success_count += 1

    print(f"成功解压 {total_count} 个文件中的 {success_count} 个")
    return (success_count, total_count)


def main() -> None:
    """
    主函数，用于解压指定目录中的所有 .gz 文件
    """

    dynamic_dir = ROOT / "data" / "ldbc-sn-bi-sf1" / "dynamic"
    static_dir = ROOT / "data" / "ldbc-sn-bi-sf1" / "static"
    if not dynamic_dir.exists() or not static_dir.exists():
        print(f"目录不存在: {dynamic_dir} 或 {static_dir}")
        print("跳过解压操作")
        return

    start_time: datetime = datetime.now()

    # 定义目录模式
    dynamic_dir_pattern: str = f"{ROOT}/data/ldbc-sn-bi-sf1/dynamic/*/*.gz"
    static_dir_pattern: str = f"{ROOT}/data/ldbc-sn-bi-sf1/static/*/*.gz"

    print(f"开始解压 .gz 文件，时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}...")

    # 查找并解压 dynamic 目录中的 .gz 文件
    print("\n处理 dynamic 目录:")
    dynamic_success, dynamic_total = find_and_extract_gzip_files(dynamic_dir_pattern)

    # 查找并解压 static 目录中的 .gz 文件
    print("\n处理 static 目录:")
    static_success, static_total = find_and_extract_gzip_files(static_dir_pattern)

    # 计算耗时
    end_time: datetime = datetime.now()
    elapsed_time = end_time - start_time

    # 打印摘要
    total_success: int = dynamic_success + static_success
    total_files: int = dynamic_total + static_total

    print("\n解压摘要:")
    print(f"- 开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"- 结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"- 耗时: {elapsed_time}")

    if total_files > 0:
        success_rate: float = (total_success / total_files) * 100
        print(
            f"- 成功解压: {total_files} 个文件中的 {total_success} 个 ({success_rate:.2f}%)。"
        )
    else:
        print("- 没有找到需要解压的文件。")


if __name__ == "__main__":
    main()
