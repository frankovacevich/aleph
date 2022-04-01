import time
import asyncio


class WaitOneStep:

    def __init__(self, time_step=1):
        self.time_step = time_step
        self.t = time.time()

    def wait(self):
        delta = time.time() - self.t

        if delta > self.time_step:
            return
        else:
            time.sleep(self.time_step - delta)
            self.t = time.time()

    async def async_wait(self):
        delta = time.time() - self.t

        if delta > self.time_step:
            return
        else:
            await asyncio.sleep(self.time_step - delta)
            self.t = time.time()
