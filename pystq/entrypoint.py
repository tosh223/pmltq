from pystq.sqs import SqsInterface
from pystq.pubsub import PubSubInterface

def execute():
    sqs = SqsInterface()
    sqs.qsize()
    pubsub = PubSubInterface()
    pubsub.qsize()
