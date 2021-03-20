import re
import json

from queuing_hub.connector.base import BaseSubscriber as Base
from queuing_hub.connector.aws import AwsSubscriber as Aws
from queuing_hub.connector.gcp import GcpSubscriber as Gcp

class Subscriber:

    def __init__(self):
        self.__connectors: list(Base) = [Aws(), Gcp()]
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

    def get(self, max_num: int) -> list:
        response = {}
        connector: Base

        for sub in self._sub_list:
            connector = self.__get_connector(sub)
            response = connector.get(sub, max_num)
            if response != {}:
                break

        return response

    @staticmethod
    def __get_connector(sub_path: str) -> Base:
        if re.search(
            r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
            sub_path
        ):
            connector = Aws()
        elif re.search(
            r'projects/[a-z0-9-]+/subscriptions/.+',
            sub_path
        ):
            connector = Gcp()
        else:
            raise ValueError(f'invalid subscription: {sub_path}')
        
        return connector
