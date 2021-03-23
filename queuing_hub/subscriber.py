import json

from queuing_hub.conn.base import BaseSub
from queuing_hub.conn.aws import AwsSub
from queuing_hub.conn.gcp import GcpSub
from queuing_hub.util import get_connector

class Subscriber:

    def __init__(self):
        self.__connectors: list(BaseSub) = [AwsSub(), GcpSub()]
        self._sub_list = []

    @property
    def sub_list(self) -> list:
        return self._sub_list

    @sub_list.setter
    def sub_list(self, sub_list: list):
        if type(sub_list) != list:
            raise TypeError('sub_list must be list type.')
        self._sub_list = sub_list

    def qsize(self) -> str:
        response = {}
        for connector in self.__connectors:
            response.update(connector.qsize())
        return json.dumps(response, indent=2)

    def pull(self, max_num: int) -> list:
        response = {}
        connector: BaseSub

        for sub in self._sub_list:
            connector = get_connector(sub)
            response = connector.pull(sub, max_num)
            if response != {}:
                break

        return response
