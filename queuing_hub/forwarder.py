from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber

class Forwarder:

    def __init__(self, sub: str, topic: str, max_num: int, gcp_credential_path, gcp_project):
        self.publisher = Publisher(
            gcp_credential_path=gcp_credential_path,
            gcp_project=gcp_project
        )
        self.subscriber = Subscriber(
            gcp_credential_path=gcp_credential_path,
            gcp_project=gcp_project
        )
        self.topic = topic
        self.sub = sub
        self.max_num = max_num

    def transport(self, ack: bool=True) -> list:
        messages = self.subscriber.pull(
            sub_list=[self.sub],
            max_num=self.max_num,
            ack=ack
        )
        responses = []
        for message in messages:
            response = self.publisher.push(
                topic_list=[self.topic],
                body=message
            )
            responses.append(response)
        return responses

    def pass_through(self):
        self.transport(ack=False)
