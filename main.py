# ruff: noqa: F403
# ruff: noqa: F405

from tests.ldbc_snb_interactive_sf01 import *
from tests.simple_test_dataset import *

if __name__ == "__main__":
    """ 测试用例 """

    # 自建测试集 - OK
    # test_triangle_forest()
    # test_more_triangle_forest()

    # ic-1/6/11/12 - OK
    # test_ic_1_on_sf01()
    # test_ic_11_on_sf01()
    # test_ic_6_on_sf01()
    # test_ic_12_on_sf01()

    # ic-4 查询不到结果
    # test_ic_4_on_sf01()

    # ic-5 查询过慢
    # test_ic_5_on_sf01()

    pass
