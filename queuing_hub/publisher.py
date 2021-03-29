import re

from queuing_hub.conn.base import BasePub
from queuing_hub.conn.aws import AwsPub
from queuing_hub.conn.gcp import GcpPub


class Publisher:

    def __init__(
        self, aws_profile_name=None,
        gcp_credential_path=None,
        gcp_project=None
    ):
        self._aws_pub = AwsPub(profile_name=aws_profile_name)
        self._gcp_pub = GcpPub(
            credential_path=gcp_credential_path,
            project=gcp_project
        )

        self.__connectors: list(BasePub) = [self._aws_pub, self._gcp_pub]
        self._topic_list = []
        for connector in self.__connectors:
            self._topic_list.extend(connector.topic_list)

    @property
    def topic_list(self) -> list:
        return self._topic_list

    def push(self, topic_list: list, body: str) -> list:
        message_ids = []
        connector: BasePub

        for topic in topic_list:
            connector = self._get_connector(topic)
            message_ids.append(connector.push(topic, body))
        return message_ids

    def _get_connector(self, topic: str) -> BasePub:
        if re.search(
            r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
            topic
        ):
            connector = self._aws_pub
        elif re.search(
            r'projects/[a-z0-9-]+/topics/.+',
            topic
        ):
            connector = self._gcp_pub
        else:
            raise ValueError(f'Invalid topic: {topic}')

        return connector
