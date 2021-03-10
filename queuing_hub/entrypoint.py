from queuing_hub.aws import AwsPublisher, AwsSubscriber
from queuing_hub.gcp import GcpPublisher, GcpSubscriber

def execute():
    aws_subscriber = AwsSubscriber()
    aws_sub_list = aws_subscriber.subscription_list
    print(aws_sub_list)
    aws_subscriber.qsize(aws_sub_list)
    print(aws_subscriber.is_empty(aws_sub_list[0]))

    gcp_subscriber = GcpSubscriber()
    gcp_sub_list = gcp_subscriber.subscription_list
    print(gcp_sub_list)
    gcp_subscriber.qsize()
    print(gcp_subscriber.is_empty(gcp_sub_list[0]))
    gcp_subscriber.purge(gcp_sub_list[0])
    gcp_subscriber.get_streaming(gcp_sub_list[2])
