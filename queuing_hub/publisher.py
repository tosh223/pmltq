import re

from queuing_hub.connector.base import BasePublisher as Base
from queuing_hub.connector.aws import AwsPublisher as Aws
from queuing_hub.connector.gcp import GcpPublisher as Gcp

class Publisher:

    def __init__(self):
        self._topic_list = []

    @property
    def topic_list(self) -> list:
        return self._topic_list

    @topic_list.setter
    def topic_list(self, topic_list: list):
        if type(topic_list) != list:
            raise TypeError('topic_list must be list type.')
        self._topic_list = topic_list

    def put(self):
        pass

    @staticmethod
    def __get_connector(sub_path: str) -> Base:
        if re.search(
            r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
            sub_path
        ):
            connector = Aws()
        elif re.search(
            r'projects/[a-z0-9-]+/topics/.+',
            sub_path
        ):
            connector = Gcp()
        else:
            raise ValueError(f'invalid subscription: {sub_path}')
        
        return connector
