class BasePublisher():
    def __init__(self):
        pass

    def put(self, topic, item, block=True, timeout=None):
        pass

    def put_nowait(self, topic, item):
        self.put(topic, item, block=False)

class BaseSubscriber():
    def __init__(self):
        pass

    def qsize(self, topic) -> int:
        pass

    def empty(self, topic) -> bool:
        pass

    def get(self, topic, block=True, timeout=None):
        pass

    def get_nowait(self, topic):
        self.get(topic, block=False)

    def tasl_done(self):
        pass

    def join(self):
        pass
