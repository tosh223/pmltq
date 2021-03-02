import os
from datetime import datetime
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from google.cloud.monitoring_v3 import query, MetricServiceClient

from pystq.base import BaseInterface

class PubSubInterface(BaseInterface):

    PROJECT = os.environ['GCP_PROJECT']
    TIMEOUT = 5.0
    METRIC_TYPE = 'pubsub.googleapis.com/subscription/num_undelivered_messages'

    def __init__(self):
        # super().__init__()
        self._monitor = MetricServiceClient()
        self._publisher = pubsub_v1.PublisherClient()
        self._subscriber = pubsub_v1.SubscriberClient()
        project_path = self._publisher.project_path(self.PROJECT)
        self._queue_list = self._publisher.list_topics(project_path)

    @property
    def queue_list(self):
        return self._queue_list

    def qsize(self):
        pubsub_query = query.Query(
            self._monitor,
            self.PROJECT,
            metric_type=self.METRIC_TYPE,
            end_time=datetime.now(),
            minutes=2   # if set 1 minute, we get nothing while creating the latest metrics.
        )
        # .select_resources(subscription_id=sub_name)

        for content in pubsub_query:
            subscription_id = content.resource.labels['subscription_id']
            count = content.points[0].value.int64_value
            print(f'{subscription_id}: {count}')

    def _callback(self, message):
        print(f'Received {message.data}.')
        if message.attributes:
            print('Attributes:')
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f'{key}: {value}')
        message.ack()

    def get(self, subscription_id):
        subscription_path = self._subscriber.subscription_path(self.PROJECT, subscription_id)
        streaming_pull_future = self._subscriber.subscribe(subscription_path, callback=self._callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=self.TIMEOUT)
            except TimeoutError:
                streaming_pull_future.cancel()
