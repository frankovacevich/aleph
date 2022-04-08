from aleph.common.wait_one_step import WaitOneStep
import unittest
import time


class TestWaitOneStep(unittest.TestCase):

    def test1(self):
        t0 = time.time()
        w = WaitOneStep("* * * * *")  # Every minute
        for i in range(0, 5):
            w.wait()
            print(time.time(), time.time() - t0)

    """
    def test2(self):
        t0 = time.time()
        w = WaitOneStep(60)
        for i in range(0, 5):
            w.wait()
            print(time.time() - t0)
    """


if __name__ == '__main__':
    unittest.main()

