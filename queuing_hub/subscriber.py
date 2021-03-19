import re

from queuing_hub.connector.base import BaseSubscriber as Base
from queuing_hub.connector.aws import AwsSubscriber as Aws
from queuing_hub.connector.gcp import GcpSubscriber as Gcp

class Subscriber:

    def __init__(self):
        self.__connectors: list(Base) = [Aws(), Gcp()]
        self.__sub_conf = {}
        self.__sub_list = []
        # path, priority

        self.__get_priority_sub()

    def qsize(self):
        response = {}
        for connector in self.__connectors:
            response.update(connector.qsize())
        return response

    def get(self, max_num):
        response = {}
        connector: Base

        for sub in self.__sub_list:
            sub_path = sub['path']
            connector = self.__get_connector(sub_path)
            response = connector.get(sub_path, max_num)
            if response != {}:
                break

        return response

    def __get_priority_sub(self):
        for i, key in enumerate(self.__sub_conf):
            subs = [x['path'] for x in self.__sub_conf if x['priority'] == i]
            sub = subs[0] if subs else None
            if sub:
                self.__sub_list.append(self.__sub_conf[key])

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
            raise Exception()
        
        return connector
