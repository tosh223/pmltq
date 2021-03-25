from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber

class Forwarder:

    def __init__(self, sub_list: list, topic_list: list, max_num: int):
        self.publisher = Publisher()
        self.subscriber = Subscriber()
        self.topic_list = topic_list
        self.sub_list = sub_list
        self.max_num = max_num

    def transport(self, ack: bool=True):
        messages = self.subscriber.pull(
            sub_list=self.sub_list,
            max_num=self.max_num,
            ack=ack
        )
        for message in messages:
            self.publisher.push(topic_list=self.topic_list, body=message)

    def pass_through(self):
        self.transport(ack=False)
