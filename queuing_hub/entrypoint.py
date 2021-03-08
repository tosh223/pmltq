from queuing_hub.aws import AwsPublisher, AwsSubscriber
from queuing_hub.gcp import GcpPublisher, GcpSubscriber

def execute():
    aws_subscriber = AwsSubscriber()
    subscription_list = aws_subscriber.subscription_list
    print(subscription_list)
    aws_subscriber.qsize(subscription_list)
    print(aws_subscriber.is_empty(subscription_list[0]))

    gcp_subscriber = GcpSubscriber()
    gcp_subscriber.qsize()
