from datetime import datetime
import json

from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber

def execute():
    pub = Publisher()
    print(pub.topic_list)
    print(pub.push(pub.topic_list, 'hey topics!'))

    sub = Subscriber()
    print(sub.sub_list)
    print(sub.qsize())
    print(sub.pull(sub.sub_list, 1))
