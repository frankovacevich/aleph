from aleph.common.local_storage import *
import unittest
import os


class TestLocalStorage(unittest.TestCase):
    storage = LocalStorage()

    def test1(self):
        self.assertEqual(self.storage.get("t1"), None)
        self.assertEqual(self.storage.get("t1", "HI"), "HI")
        self.assertEqual(self.storage.set("t1", 66), 66)
        self.assertEqual(self.storage.get("t1"), 66)
        self.assertEqual(self.storage.set("t1", 32), 32)
        self.assertEqual(self.storage.get("t1"), 32)

        self.assertDictEqual(self.storage.get("t2", {"x": 1, "y": 2}), {"x": 1, "y": 2})
        self.assertDictEqual(self.storage.set("t2", {"x": 1, "y": 2}), {"x": 1, "y": 2})
        self.assertDictEqual(self.storage.get("t2"), {"x": 1, "y": 2})

        self.assertDictEqual(self.storage.set("t2", {"x": {"y": 5}}), {"x": {"y": 5}})
        self.assertDictEqual(self.storage.get("t2"), {"x": {"y": 5}})

    def test2(self):
        if os.path.isfile("test.json"): os.remove("test.json")
        self.storage = JsonLocalStorage("test.json")
        self.test1()
        os.remove("test.json")

    def test3(self):
        if os.path.isfile("test.pickle"): os.remove("test.pickle")
        self.storage = FileLocalStorage("test.pickle")
        self.test1()
        os.remove("test.pickle")

    def test4(self):
        if os.path.isfile("test.db"): os.remove("test.db")
        self.storage = SqliteDictLocalStorage("test.db")
        self.test1()
        os.remove("test.db")

    def test5(self):
        self.storage = RedisLocalStorage("test")
        self.storage.red.delete("testt1", "testt2")
        self.test1()


if __name__ == '__main__':
    unittest.main()
