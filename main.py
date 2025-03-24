from tests.ldbc_snb_interactive_sf01 import (
    test_ic_1_on_sf01,
    test_ic_6_on_sf01,
    test_ic_11_on_sf01,
)
from tests.simple_test_dataset import test_more_triangle_forest, test_triangle_forest

if __name__ == "__main__":
    test_triangle_forest()
    test_more_triangle_forest()

    test_ic_1_on_sf01()
    test_ic_11_on_sf01()
    test_ic_6_on_sf01()
