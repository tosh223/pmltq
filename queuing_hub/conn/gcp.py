import os
from datetime import datetime
from concurrent.futures import TimeoutError as FuturesTimeoutError

from google.cloud import pubsub_v1
from google.cloud.monitoring_v3 import query, MetricServiceClient
from google.oauth2.service_account import Credentials

from queuing_hub.conn.base import BasePub, BaseSub

PROJECT = os.environ['GCP_PROJECT']


class GcpPub(BasePub):

    def __init__(self, credential_path=None, project=None):
        super().__init__()

        if credential_path:
            credentials = Credentials.from_service_account_file(
                filename=credential_path
            )
        else:
            credentials = None

        if project:
            self._project = project
        else:
            self._project = PROJECT

        self._publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self._pub_organizer = pubsub_v1.publisher.client.publisher_client \
            .PublisherClient(credentials=credentials)

        # topics
        project_path = self._pub_organizer.common_project_path(self._project)
        topics_obj = self._pub_organizer.list_topics(project=project_path)
        self._topic_list = [queue.name for queue in topics_obj]

    @property
    def topic_list(self) -> list:
        return self._topic_list

    def push(self, topic: str, body: str) -> None:
        future = self._publisher.publish(topic, body.encode())
        # future.add_done_callback(self._callback)
        return future.result()

    @staticmethod
    def _callback(future):
        message_id = future.result()
        print(f'MessageId {message_id} has published! ')


class GcpSub(BaseSub):

    TIMEOUT = 5.0
    METRIC_TYPE = 'pubsub.googleapis.com/subscription/num_undelivered_messages'

    def __init__(self, credential_path=None, project=None):
        super().__init__()

        if credential_path:
            credentials = Credentials.from_service_account_file(
                filename=credential_path
            )
        else:
            credentials = None

        if project:
            self._project = project
        else:
            self._project = PROJECT

        self._subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
        self._sub_organizer = pubsub_v1.subscriber.client.subscriber_client \
            .SubscriberClient(credentials=credentials)

        # subscriptions
        project_path = self._sub_organizer.common_project_path(self._project)
        sub_obj = self._sub_organizer.list_subscriptions(project=project_path)
        self._sub_list = [queue.name for queue in sub_obj]

    def __del__(self):
        self._subscriber.close()

    @property
    def sub_list(self) -> list:
        return self._sub_list

    def qsize(self, sub_list: list = None) -> dict:
        response = {'gcp': {}}
        if not sub_list:
            sub_list = self._sub_list

        query_results = query.Query(
            client=MetricServiceClient(),
            project=self._project,
            metric_type=self.METRIC_TYPE,
            end_time=datetime.now(),
            minutes=2
            # if set 1 minute, we get nothing
            # while creating the latest metrics.
        )

        for result in self.__read_metric(query_results=query_results):
            response['gcp'][result['subscription']] = result['value']

        return response

    def is_empty(self, sub: str) -> bool:
        return self.qsize([sub])[sub] == 0

    def pull(self, sub: str, max_num: int = 1, ack: bool = False) -> list:
        messages = []
        response = self._sub_organizer.pull(
            request={
                'subscription': sub,
                "max_messages": max_num,
            }
        )

        for msg in response.received_messages:
            messages.append(msg.message.data.decode())

        if ack:
            self._ack(sub=sub, messages=response.received_messages)

        return messages

    def pull_streaming(self, sub: str) -> None:
        streaming_pull_future = self._subscriber.subscribe(
            sub,
            callback=self.__streaming_pull_callback
        )
        print(f"Listening for messages on {sub}..\n")

        with self._subscriber:
            try:
                streaming_pull_future.result(timeout=self.TIMEOUT)
            except FuturesTimeoutError:
                streaming_pull_future.cancel()

    def purge(self, sub: str) -> None:
        seek_request = pubsub_v1.types.pubsub_gapic_types \
            .SeekRequest(subscription=sub, time=datetime.now())
        self._sub_organizer.seek(request=seek_request)

    def _ack(self, sub: str, messages: list) -> None:
        ack_ids = [msg.ack_id for msg in messages]
        self._sub_organizer.acknowledge(
            request={
                "subscription": sub,
                "ack_ids": ack_ids,
            }
        )

    def __read_metric(self, query_results: query.Query) -> dict:
        for content in query_results:
            sub_id = content.resource.labels['subscription_id']
            sub = self._sub_organizer.subscription_path(self._project, sub_id)
            yield {
                'subscription': sub,
                'value': content.points[0].value.int64_value
            }

    def __streaming_pull_callback(self, message):
        print(f'Received {message.data.decode()}.')
        if message.attributes:
            print('Attributes:')
            for key in message.attributes:
                value = message.attributes.get(key)
                print(f'{key}: {value}')
        message.ack()
