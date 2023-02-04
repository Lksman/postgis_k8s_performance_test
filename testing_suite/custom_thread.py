from threading import Thread

class CustomThread(Thread):
    def __init__(self, **kwargs):
        Thread.__init__(self, **kwargs)
        self.value = None

    def run(self):
        self.value = self._target(*self._args, **self._kwargs)