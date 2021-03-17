from queuing_hub.connector.base import BaseSubscriber as Base
from queuing_hub.connector.aws import AwsSubscriber as Aws
from queuing_hub.connector.gcp import GcpSubscriber as Gcp

class Subscriber:

    connectors: list(Base) = [Aws(), Gcp()]

    def __init__(self):
        self.__sub_conf = {}
        self.__sub_list = []
        # type, path, priority

        self.__get_priority_sub()

    def qsize(self):
        response = {}
        for connector in self.connectors:
            response.update(connector.qsize())
        return response

    def get(self, max_num):
        response = {}
        connector: Base

        for sub in self.__sub_list:
            connector = self.__get_connector(sub['type'])
            if connector is None:
                raise Exception()
            response = connector.get(sub['path'], max_num)
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
    def __get_connector(type_str: str):
        conn_type = str(type_str).upper()
        if conn_type == 'AWS':
            connector = Aws()
        elif conn_type == 'GCP':
            connector = Gcp()
        else:
            connector = None
        
        return connector
