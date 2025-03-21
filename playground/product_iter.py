from itertools import product

if __name__ == "__main__":
    a: list[list[int]] = [[1, 2, 3], [4, 5, 6], [], [7, 8, 9, 10], [11]]
    a = [e for e in a if e]

    for comb in product(*a):
        print(comb)
