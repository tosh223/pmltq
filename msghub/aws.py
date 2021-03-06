import boto3

from msghub.base import BasePublisher, BaseSubscriber

class AwsBase():

    def __init__(self, client=None):
        if client:
            self._client = client
        else:
            session = boto3.Session()
            self._client = session.client('sqs')
        self._topic_list = self._client.list_queues()['QueueUrls']

    @property
    def topic_list(self):
        return self._topic_list


class AwsPublisher(AwsBase, BasePublisher):

    def __init__(self, client=None):
        AwsBase.__init__(self, client=client)
        BasePublisher.__init__(self)

    def put(self, topic):
        pass


class AwsSubscriber(AwsBase, BaseSubscriber):

    ATTRIBUTE_NAMES = [
        'ApproximateNumberOfMessages',
        # 'ApproximateNumberOfMessagesDelayed',
        # 'ApproximateNumberOfMessagesNotVisible',
        # 'DelaySeconds',
        # 'MessageRetentionPeriod',
        # 'ReceiveMessageWaitTimeSeconds',
        # 'VisibilityTimeout'
    ]

    def __init__(self, client=None):
        AwsBase.__init__(self, client=client)
        BaseSubscriber.__init__(self)

    def _get_attributes(self, queue_url, attribute_names):
        response= self._client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=attribute_names
        )
        return response['Attributes']

    def qsize(self, subscription_list :list=None):
        if not subscription_list:
            subscription_list = self._topic_list

        for subscription in subscription_list:
            attributes = self._get_attributes(subscription, self.ATTRIBUTE_NAMES)
            count = attributes[self.ATTRIBUTE_NAMES[0]]
            print(f'{subscription}: {count}')

    def empty(self, topic) -> bool:
        pass

    def get(self, queue_url, block=True, timeout=None):
        return self._client.receive_message(QueueUrl=queue_url)

    def tasl_done(self):
        pass

    def join(self):
        pass
