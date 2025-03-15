TEST = """\
This is a test
    - of a multiline (path: `{}`)
"""

if __name__ == "__main__":
    print(TEST.format(__file__))
