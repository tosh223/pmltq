class BasePublisher():
    def __init__(self):
        pass

    def put(self, topic, body):
        pass


class BaseSubscriber():
    def __init__(self):
        pass

    def qsize(self, subscription_list):
        pass

    def is_empty(self, subscription):
        pass

    def get(self, subscription, max_num):
        pass

    def ack(self, subscription, messages):
        pass
