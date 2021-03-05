from pystq.aws import AwsPublisher, AwsSubscriber
from pystq.gcp import PubSubInterface

def execute():
    aws_publisher = AwsPublisher()
    aws_publisher.qsize()
    # pubsub = PubSubInterface()
    # pubsub.qsize()
