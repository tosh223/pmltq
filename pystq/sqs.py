import boto3

from pystq.base import BaseInterface

class SqsInterface(BaseInterface):

    ATTRIBUTE_NAMES = [
        'ApproximateNumberOfMessages',
        # 'ApproximateNumberOfMessagesDelayed',
        # 'ApproximateNumberOfMessagesNotVisible',
        # 'DelaySeconds',
        # 'MessageRetentionPeriod',
        # 'ReceiveMessageWaitTimeSeconds',
        # 'VisibilityTimeout'
    ]

    def __init__(self):
        super().__init__()
        session = boto3.Session()
        self._client = session.client('sqs')
        self._queue_list = self._client.list_queues()['QueueUrls']

    def __get_queue_attributes(self, queue_url, attribute_names):
        response= self._client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=attribute_names
        )
        return response['Attributes']

    @property
    def queue_list(self):
        return self._queue_list

    def qsize(self, queue_list :list=None):
        if not queue_list:
            queue_list = self._queue_list

        for queue_url in queue_list:
            self._attributes = self.__get_queue_attributes(queue_url, self.ATTRIBUTE_NAMES)
            count = self._attributes[self.ATTRIBUTE_NAMES[0]]
            print(f'{queue_url}: {count}')

    def get(self, queue_url, block=True, timeout=None):
        return self._client.receive_message(QueueUrl=queue_url)
