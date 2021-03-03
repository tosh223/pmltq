import os
from datetime import datetime
from concurrent.futures import TimeoutError

from google.cloud.pubsub_v1.publisher.client import publisher_client
from google.cloud.pubsub_v1.subscriber.client import subscriber_client
from google.cloud.monitoring_v3 import query, MetricServiceClient

from pystq.base import BaseInterface

class PubSubInterface(BaseInterface):

    PROJECT = os.environ['GCP_PROJECT']
    TIMEOUT = 5.0
    METRIC_TYPE = 'pubsub.googleapis.com/subscription/num_undelivered_messages'

    def __init__(self):
        super().__init__()
        self._monitor = MetricServiceClient()
        self._publisher = publisher_client.PublisherClient()
        # self._subscriber = subscriber_client.SubscriberClient()
        project_path = self._publisher.common_project_path(self.PROJECT)

        # topics
        self._queue_list = [queue.name for queue in self._publisher.list_topics(project=project_path)]

        # subscriptions

    @property
    def queue_list(self):
        return self._queue_list

    def qsize(self, queue_list :list=None):
        if not queue_list:
            queue_list = self._queue_list

        pubsub_query = query.Query(
            self._monitor,
            self.PROJECT,
            metric_type=self.METRIC_TYPE,
            end_time=datetime.now(),
            minutes=2   # if set 1 minute, we get nothing while creating the latest metrics.
        )
        # .select_resources(subscription_id=queue_url)

        # queue_dict = dict(zip([str(queue).split('/')[-1] for queue in queue_list], queue_list))

        for content in pubsub_query:
            subscription_id = content.resource.labels['subscription_id']
            # queue_url = queue_dict[subscription_id]
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