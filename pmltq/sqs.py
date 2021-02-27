import boto3

from base import QueueInterface

class Sqs():

    def __init__(self):
        # boto3 session
        session = boto3.Session()
        self.__client = session.client('sqs')
        self.__queue_url_list = self.__client.list_queues()['QueueUrls']

    @property
    def client(self):
        return self.__client

    @property
    def queue_url_list(self):
        return self.__queue_url_list

class SqsInterface(QueueInterface):

    ATTRIBUTE_NAMES = [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesDelayed',
        'ApproximateNumberOfMessagesNotVisible',
        'DelaySeconds',
        'MessageRetentionPeriod',
        'ReceiveMessageWaitTimeSeconds',
        'VisibilityTimeout'
    ]

    def __init__(self, client, queue_url):
        self._clinet = client
        self._queue_url = queue_url
        self._attributes = self.__get_queue_attributes()

    def __get_queue_attributes(self):
        response= self._clinet.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=self.ATTRIBUTE_NAMES
        )
        return response['Attributes']

    def qsize(self):
        return self._attributes['ApproximateNumberOfMessages']

    def get(self, block=True, timeout=None):
        return self._clinet.receive_message(QueueUrl=self._queue_url)

if __name__ == "__main__":
    sqs = Sqs()
    interface = SqsInterface(sqs.client, sqs.queue_url_list[0])
    response = interface.qsize()
    print(response)
