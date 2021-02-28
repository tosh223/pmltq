import os
from datetime import datetime
from concurrent.futures import TimeoutError

from google.cloud import monitoring_v3, pubsub_v1
from google.cloud.monitoring_v3 import query

PROJECT = os.environ['GCP_PROJECT']
TIMEOUT = 5.0

def qsize():
    client = monitoring_v3.MetricServiceClient()
    pubsub_query = query.Query(
        client,
        PROJECT,
        # metric_type='pubsub.googleapis.com/subscription/num_undelivered_messages',
        metric_type='pubsub.googleapis.com/subscription/oldest_unacked_message_age',
        end_time=datetime.now(),
        minutes=2   # if set 1 minute, we get nothing while creating the latest metrics.
    )
    # .select_resources(subscription_id=sub_name)

    for content in pubsub_query:
        # print(content)
        subscription_id = content.resource.labels['subscription_id']
        count = content.points[0].value.int64_value
        print(f'{subscription_id}: {count}')

def callback(message):
    print(f'Received {message.data}.')
    if message.attributes:
        print('Attributes:')
        for key in message.attributes:
            value = message.attributes.get(key)
            print(f'{key}: {value}')
    message.ack()

def pull(subscription_id):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_pull_future.cancel()
