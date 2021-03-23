from queuing_hub.conn.base import BasePub
from queuing_hub.conn.aws import AwsPub
from queuing_hub.conn.gcp import GcpPub
from queuing_hub.util import get_connector

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

    def push(self):
        pass
