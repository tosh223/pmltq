class BasePub():
    def __init__(self):
        pass

    def push(self, topic: str, body: str):
        pass


class BaseSub():
    def __init__(self):
        pass

    def qsize(self, subscription_list):
        pass

    def is_empty(self, subscription):
        pass

    def pull(self, sub: str, max_num: int=1):
        pass

    def ack(self, subscription, messages):
        pass
