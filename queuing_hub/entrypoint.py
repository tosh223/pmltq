from datetime import datetime
import json

from queuing_hub.subscriber import Subscriber

def execute():
    sub = Subscriber()
    print(sub.qsize())
    sub.sub_list = ['projects/alert-tine-289008/subscriptions/test-topic-sub']

    print(sub.pull(1))

    # aws_publisher = AwsPublisher()
    # aws_topic_list = aws_publisher.topic_list
    # _ = aws_publisher.put(aws_topic_list[0], str(datetime.now()))

    # aws_subscriber = AwsSubscriber()
    # aws_sub_list = aws_subscriber.subscription_list
    # print(aws_subscriber.qsize(aws_sub_list))
    # print(aws_subscriber.is_empty(aws_sub_list[0]))

    # gcp_publisher = GcpPublisher()
    # gcp_topic_list = gcp_publisher.topic_list
    # print(gcp_topic_list)
    # gcp_publisher.put(gcp_topic_list[0], str(datetime.now()))

    # gcp_subscriber = GcpSubscriber()
    # gcp_sub_list = gcp_subscriber.subscription_list
    # print(gcp_sub_list)
    # print(gcp_subscriber.qsize())
    # print(gcp_subscriber.is_empty(gcp_sub_list[0]))
    # print(gcp_subscriber.get(gcp_sub_list[0]))
    # gcp_subscriber.purge(gcp_sub_list[1])
    # gcp_subscriber.get_streaming(gcp_sub_list[2])
