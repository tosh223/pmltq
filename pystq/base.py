class BaseInterface():
    def __init__(self):
        pass

    def qsize(self) -> int:
        pass

    def empty(self) -> bool:
        pass

    def put(self, item, block=True, timeout=None):
        pass

    def put_nowait(self, item):
        self.put(item, block=False)
    
    def get(self, block=True, timeout=None):
        pass

    def get_nowait(self):
        self.get(block=False)

    def tasl_done(self):
        pass

    def join(self):
        pass
