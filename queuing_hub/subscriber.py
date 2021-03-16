from queuing_hub.connector.base import BaseSubscriber as Base
from queuing_hub.connector.aws import AwsSubscriber as Aws
from queuing_hub.connector.gcp import GcpSubscriber as Gcp

class Subscriber:
    def __init__(self):
        self.__connectors: list(Base) = [Aws(), Gcp()]
        self.__subscriptions = {}

    def qsize(self):
        response = {}
        for connector in self.__connectors:
            response.update(connector.qsize())
        return response

    def get(self, max_num):
        response = {}
        connector: Base

        # primary subscriber
        values = [x['path'] for x in self.__subscriptions if x['priority'] == 0]
        primary = values[0] if values else None
        connector = self.__get_connector(primary)
        if connector is None:
            return None
        response = connector.get(primary, max_num)

        # secondary subscriber

        return response

    @staticmethod
    def __get_connector(subscription):
        conn_type = str(subscription['type']).upper()
        if conn_type == 'AWS':
            connector = Aws()
        elif conn_type == 'GCP':
            connector = Gcp()
        else:
            connector = None
        
        return connector
