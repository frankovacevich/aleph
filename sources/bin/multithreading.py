"""
Use the MultiThreading class for services that require multiple threads

Usage:
-----

MT = MultiThreading()

def do_something_1():
    return

def do_something_2():
    return

MT.add_process(do_something_1)
MT.add_process(do_something_2)
MT.start()

"""


from multiprocessing import Process


class MultiThreading:

    def __init__(self):
        self.threads = []

    def add_process(self, function):
        self.threads.append(function)

    def start(self):
        processes = []

        for thread in self.threads:
            p = Process(target=thread)
            p.start()
            processes.append(p)

        for process in processes:
            process.join()
