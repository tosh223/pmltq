class BasePub():
    def __init__(self):
        pass

    def push(self, topic, body):
        pass


class BaseSub():
    def __init__(self):
        pass

    def qsize(self, subscription_list):
        pass

    def is_empty(self, subscription):
        pass

    def pull(self, subscription, max_num):
        pass

    def ack(self, subscription, messages):
        pass
