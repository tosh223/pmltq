from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber

class Forwarder:

    def __init__(self):
        self.publisher = Publisher()
        self.subscriber = Subscriber()

    def forward(self, sub_list: list, topic_list: list, max_num: int, ack: bool=True):
        messages = self.subscriber.pull(sub_list=sub_list, max_num=max_num, ack=ack)
        for message in messages:
            self.publisher.push(topic_list=topic_list, body=message)

    def forward_nack(self, sub_list: list, topic_list: list, max_num: int):
        self.forward(sub_list=sub_list, topic_list=topic_list, max_num=max_num, ack=False)
