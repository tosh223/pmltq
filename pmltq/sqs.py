import boto3

class Sqs():

    ATTRIBUTE_NAMES = [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesDelayed',
        'ApproximateNumberOfMessagesNotVisible',
        'DelaySeconds',
        'MessageRetentionPeriod',
        'ReceiveMessageWaitTimeSeconds',
        'VisibilityTimeout'
    ]

    def __init__(self):
        # boto3 session
        session = boto3.Session()
        self._sqs = session.client('sqs')
        self.queue_url_list = self.__list_queues()

    def __list_queues(self):
        response = self._sqs.list_queues()
        return response['QueueUrls']

    def receive_message(self, queue_url):
        return self._sqs.receive_message(QueueUrl=queue_url)

    def get_queue_attributes(self):
        attribures = {}
        for queue_url in self.queue_url_list:
            response= self._sqs.get_queue_attributes(
                 QueueUrl=queue_url,
                 AttributeNames=self.ATTRIBUTE_NAMES
            )
            attribures[queue_url] = response['Attributes']
        return attribures

if __name__ == "__main__":
    sqs = Sqs()
    response = sqs.get_queue_attributes()
    print(response)
