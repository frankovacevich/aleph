import unittest
from aleph import Connection


class TestSum(unittest.TestCase):

    def test1(self):
        C = Connection()
        a = C.__clean_read_args__("")
        b = C.__clean_read_args__("", **a)
        self.assertDictEqual(a, b)


if __name__ == '__main__':
    unittest.main()
