from aleph import Connection
import unittest


def remove_t(data):
    ndata = []
    for record in data:
        ndata.append({r: record[r] for r in record if r not in ["t"]})
    return ndata


class TestConn(Connection):

    def __init__(self):
        super(TestConn, self).__init__()
        self.report_by_exception = True
        self.last = None

    def write(self, key, data):
        self.last = data


class TestReadRequests(unittest.TestCase):

    def test1(self):
        conn = TestConn()

        conn.safe_write("", {"a": 1, "b": 2, "c": 3})
        self.assertListEqual(remove_t(conn.last), [{"a": 1, "b": 2, "c": 3}])

        conn.safe_write("", {"a": 2, "b": 2, "c": 3})
        self.assertListEqual(remove_t(conn.last), [{"a": 2}])

        conn.safe_write("", {"a": 2, "c": 4})
        self.assertListEqual(remove_t(conn.last), [{"c": 4}])

        conn.safe_write("", {"b": 99})
        self.assertListEqual(remove_t(conn.last), [{"b": 99}])

    def test2(self):
        conn = TestConn()

        conn.safe_write("", [{"x": 1, "y": 2}, {"x": 9, "y": 2}])
        self.assertListEqual(remove_t(conn.last), [{"x": 1, "y": 2}, {"x": 9}])

        conn.safe_write("", [{"x": 9, "y": 9}])
        self.assertListEqual(remove_t(conn.last), [{"y": 9}])

        conn.safe_write("", [{"x": 9, "y": 9}, {"x": 10, "y": 9}])
        self.assertListEqual(remove_t(conn.last), [{"x": 10}])

        conn.safe_write("", [{"x": 10}])  # This should not trigger the "write" function
        self.assertListEqual(remove_t(conn.last), [{"x": 10}])

    def test3(self):
        conn = TestConn()
        import time
        t0 = time.time() - 30
        t1 = time.time()

        conn.safe_write("", [{"x": 1, "y": 2, "t": t0}, {"x": 9, "y": 2, "t": t0}])
        self.assertListEqual(remove_t(conn.last), [{"x": 1, "y": 2}, {"x": 9}])

        conn.safe_write("", [{"x": 9, "y": 9, "t": t1}])
        self.assertListEqual(remove_t(conn.last), [{"y": 9}])

    def test4(self):
        conn = TestConn()

        conn.safe_write("", [
            {"id_": 1, "u": 100, "v": 120},
            {"id_": 2, "u": 100, "v": 121},
            {"id_": 3, "u": 102, "v": 121}
        ])
        self.assertListEqual(remove_t(conn.last), [
            {'id_': 1, 'u': 100, 'v': 120},
            {'id_': 2, 'u': 100, 'v': 121},
            {'id_': 3, 'u': 102, 'v': 121}
        ])

        conn.safe_write("", [
            {"id_": 1, "u": 100, "v": 120},
            {"id_": 2, "u": 100, "v": 121},
            {"id_": 3, "u": 555, "v": 444}
        ])
        self.assertListEqual(remove_t(conn.last), [
            {"id_": 3, "u": 555, "v": 444}
        ])

        conn.safe_write("", [
            {'id_': 1, 'u': 1,   'v': 120},
            {'id_': 2, 'u': 100, 'v': 121},
            {'id_': 3, 'u': 102, 'v': 121}
        ])
        self.assertListEqual(remove_t(conn.last), [
            {'id_': 1, 'u': 1,   'v': 120},
            {'id_': 3, 'u': 102, 'v': 121}
        ])

    def test5(self):
        conn = TestConn()
        import time
        t0 = time.time() - 30
        t1 = time.time()

        conn.safe_write("", [
            {"id_": 1, "u": 100, "v": 120, "t": t0},
            {"id_": 2, "u": 100, "v": 121, "t": t0},
            {"id_": 3, "u": 102, "v": 121, "t": t0}
        ])

        conn.safe_write("", [
            {"id_": 1, "u": 100, "v": 120, "t": t1},
            {"id_": 2, "u": 100, "v": 121, "t": t1},
            {"id_": 3, "u": 555, "v": 444, "t": t1}
        ])
        self.assertListEqual(remove_t(conn.last), [
            {"id_": 3, "u": 555, "v": 444}
        ])

        conn.safe_write("", [
            {"id_": 1, "u": 100, "v": 120},
            {"id_": 2, "u": 100, "v": 121},
            {"id_": 3, "u": 555, "v": 444}
        ])
        self.assertListEqual(remove_t(conn.last), [
            {"id_": 1, "u": 100, "v": 120},
            {"id_": 2, "u": 100, "v": 121},
            {"id_": 3, "u": 555, "v": 444}
        ])


if __name__ == '__main__':
    unittest.main()
