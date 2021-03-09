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

    def get(self, subscription):
        pass

    def task_done(self):
        pass

    def join(self):
        pass
