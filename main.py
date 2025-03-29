# ruff: noqa: F403
# ruff: noqa: F405

from tests.complex_sf01 import *
from tests.simple_sf01 import *
from tests.simple_test_dataset import *


def succeeded():
    """成功的测试用例"""

    # 自建测试集 - OK
    #
    # test_triangle_forest()
    # test_more_triangle_forest()

    # ic-1/6/11/12 - OK
    #
    # test_ic_1_on_sf01()
    # test_ic_11_on_sf01()
    # test_ic_6_on_sf01()
    # test_ic_12_on_sf01()

    # is-3 - OK
    #
    # test_is_3_on_sf01()
    # test_is_3_double_directed_knows_on_sf01()
    test_is_3_reversed_directed_knows_on_sf01()

    # is-1 - OK
    #
    # test_is_1_on_sf01()

    # ic-4 - OK
    #
    # test_ic_4_on_sf01()
    # test_minimized_ic_4()

    # 自建 ic-5 - OK
    #
    # test_minimized_ic_5()


def failed():
    """失败的测试用例"""

    # 原始 ic-5 查询过慢
    #
    test_ic_5_on_sf01()


if __name__ == "__main__":
    succeeded()
    # failed()
    pass
