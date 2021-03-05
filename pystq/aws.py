import boto3

from pystq.base import BaseTopic, BasePublisher, BaseSubscriber

class AwsTopic(BaseTopic):

    def __init__(self, client=None):
        super().__init__()

        if not client:
            session = boto3.Session()
            self._client = session.client('sqs')
        else:
            self._client = client
        self._queue_list = self._client.list_queues()['QueueUrls']

    def get_queue_attributes(self, queue_url, attribute_names):
        response= self._client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=attribute_names
        )
        return response['Attributes']

    @property
    def queue_list(self):
        return self._queue_list

class AwsPublisher(BasePublisher):

    ATTRIBUTE_NAMES = [
        'ApproximateNumberOfMessages',
        # 'ApproximateNumberOfMessagesDelayed',
        # 'ApproximateNumberOfMessagesNotVisible',
        # 'DelaySeconds',
        # 'MessageRetentionPeriod',
        # 'ReceiveMessageWaitTimeSeconds',
        # 'VisibilityTimeout'
    ]

    def __init__(self, topic, client=None):
        super().__init__()
        session = boto3.Session()
        self._client = session.client('sqs')
    
    def put(self):
        pass


class AwsSubscriber(BaseSubscriber):

    def __init__(self, topic, client=None):
        super().__init__()
        if not client:
            session = boto3.Session()
            self._client = session.client('sqs')
        else:
            self._client = client

    def get(self, queue_url, block=True, timeout=None):
        return self._client.receive_message(QueueUrl=queue_url)
