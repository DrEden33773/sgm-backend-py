# ruff: noqa: F403
# ruff: noqa: F405

from tests.bi import *
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
    # test_is_3_reversed_directed_knows_on_sf01()

    # is-1 - OK
    #
    # test_is_1_on_sf01()

    # ic-4 - OK
    #
    # test_ic_4_on_sf01()
    # test_minimized_ic_4()

    # ic-5 - OK (原始 ic-5 稍微有点慢, 不过正确的查出来了)
    #
    test_ic_5_on_sf01()
    # test_minimized_ic_5()

    # bi-3 - OK
    test_bi_3_on_sf01()

    # test_minimized_bi_6()


def failed():
    """失败的测试用例"""

    # 原始 bi-6 查询过慢
    #
    # test_bi_6_on_sf01()


if __name__ == "__main__":
    # succeeded()
    failed()
    pass
