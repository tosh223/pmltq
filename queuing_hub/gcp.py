import os
from datetime import datetime
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1
from google.cloud.monitoring_v3 import query, MetricServiceClient

from queuing_hub.base import BasePublisher, BaseSubscriber

PROJECT = os.environ['GCP_PROJECT']


class GcpPublisher(BasePublisher):

    def __init__(self):
        super().__init__()
        self._publisher = pubsub_v1.PublisherClient()
        self._pub_client = pubsub_v1.publisher.client.publisher_client.PublisherClient()

        # topics
        project_path = self._pub_client.common_project_path(PROJECT)
        self._topic_list = [queue.name for queue in self._pub_client.list_topics(project=project_path)]

    @property
    def topic_list(self):
        return self._topic_list

    def put(self, topic, body):
        pass


class GcpSubscriber(BaseSubscriber):

    TIMEOUT = 5.0
    METRIC_TYPE = 'pubsub.googleapis.com/subscription/num_undelivered_messages'

    def __init__(self):
        super().__init__()
        self._subscriber = pubsub_v1.SubscriberClient()
        self._sub_client = pubsub_v1.subscriber.client.subscriber_client.SubscriberClient()

        # subscriptions
        project_path = self._sub_client.common_project_path(PROJECT)
        self._subscription_list = [queue.name for queue in self._sub_client.list_subscriptions(project=project_path)]

    def __del__(self):
        self._subscriber.close()

    @property
    def subscription_list(self):
        return self._subscription_list

    def qsize(self, subscription_list :list=None):
        if not subscription_list:
            subscription_list = self._subscription_list

        pubsub_query = query.Query(
            MetricServiceClient(),
            PROJECT,
            metric_type=self.METRIC_TYPE,
            end_time=datetime.now(),
            minutes=2   # if set 1 minute, we get nothing while creating the latest metrics.
        )

        for content in pubsub_query:
            subscription = content.resource.labels['subscription_id']
            subscription_path = self._sub_client.subscription_path(PROJECT, subscription)
            count = content.points[0].value.int64_value
            print(f'{subscription_path}: {count}')

    def get(self, subscription):
        subscription_path = self._sub_client.subscription_path(PROJECT, subscription)
        streaming_pull_future = self._subscriber.subscribe(subscription_path, callback=self.__callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=self.TIMEOUT)
            except TimeoutError:
                streaming_pull_future.cancel()

    def is_empty(self, subscription) -> bool:
        pass

    def tasl_done(self):
        pass

    def join(self):
        pass

    def __callback(self, message):
        print(f'Received {message.data}.')
        if message.attributes:
            print('Attributes:')
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f'{key}: {value}')
        message.ack()
