from queuing_hub.aws import AwsPublisher, AwsSubscriber
from queuing_hub.gcp import GcpPublisher, GcpSubscriber

def execute():
    aws_subscriber = AwsSubscriber()
    aws_subscriber.qsize()
    gcp_subscriber = GcpSubscriber()
    gcp_subscriber.qsize()
