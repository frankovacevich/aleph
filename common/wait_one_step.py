import time


class WaitOneStep:
    def __init__(self, time_step=1):
        self.time_step = time_step
        self.first_step = True
        return

    def step(self, t):
        if self.time_step == 0: return time.time()
        if t == 0: return time.time()
        delta = time.time() - t
        if self.first_step: self.first_step = False
        elif delta > self.time_step: pass
        else: time.sleep(self.time_step - delta)
        return time.time()
