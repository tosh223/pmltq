from queuing_hub.publisher import Publisher
from queuing_hub.subscriber import Subscriber


class Forwarder:

    def __init__(
        self, sub: str, topic: str, max_num: int = 1,
        aws_profile_name=None,
        gcp_credential_path=None,
        gcp_project=None
    ):
        self.publisher = Publisher(
            aws_profile_name=aws_profile_name,
            gcp_credential_path=gcp_credential_path,
            gcp_project=gcp_project
        )
        self.subscriber = Subscriber(
            aws_profile_name=aws_profile_name,
            gcp_credential_path=gcp_credential_path,
            gcp_project=gcp_project
        )
        self.topic = topic
        self.sub = sub
        self.max_num = max_num

    def transport(self, ack: bool = True) -> list:
        messages = self.subscriber.pull(
            sub_list=[self.sub],
            max_num=self.max_num,
            ack=ack
        )

        responses = []
        if not messages:
            return responses

        for message in messages:
            response = self.publisher.push(
                topic_list=[self.topic],
                body=message
            )
            responses.append(response)
        return responses

    def pass_through(self):
        return self.transport(ack=False)
