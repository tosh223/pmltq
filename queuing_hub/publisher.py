import re

from queuing_hub.conn.base import BasePub
from queuing_hub.conn.aws import AwsPub
from queuing_hub.conn.gcp import GcpPub
from queuing_hub.util import get_connector

class Publisher:

    def __init__(self):
        self.__connectors: list(BasePub) = [AwsPub(), GcpPub()]
        self._topic_list = []
        for connector in self.__connectors:
            self._topic_list.extend(connector.topic_list)

    @property
    def topic_list(self) -> list:
        return self._topic_list

    def push(self, topic_list: list, body: str):
        response = []
        connector: BasePub

        for topic in topic_list:
            connector = self.__get_connector(topic)
            response.append(connector.push(topic, body))
        return response

    @staticmethod
    def __get_connector(topic: str) -> BasePub:
        if re.search(
            r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
            topic
        ):
            connector = AwsPub()
        elif re.search(
            r'projects/[a-z0-9-]+/topics/.+',
            topic
        ):
            connector = GcpPub()
        else:
            raise ValueError(f'Invalid topic: {topic}')
        
        return connector
