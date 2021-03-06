from msghub.aws import AwsPublisher, AwsSubscriber
from msghub.gcp import GcpPublisher, GcpSubscriber

def execute():
    aws_subscriber = AwsSubscriber()
    aws_subscriber.qsize()
    gcp_subscriber = GcpSubscriber()
    gcp_subscriber.qsize()
