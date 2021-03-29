import json
import re

from queuing_hub.conn.base import BaseSub
from queuing_hub.conn.aws import AwsSub
from queuing_hub.conn.gcp import GcpSub


class Subscriber:

    def __init__(
        self, aws_profile_name=None,
        gcp_credential_path=None, gcp_project=None
    ):
        self._aws_sub = AwsSub(profile_name=aws_profile_name)
        self._gcp_sub = GcpSub(
            credential_path=gcp_credential_path,
            project=gcp_project
        )

        self.__connectors: list(BaseSub) = [self._aws_sub, self._gcp_sub]
        self._sub_list = []
        for connector in self.__connectors:
            self._sub_list.extend(connector.sub_list)

    @property
    def sub_list(self) -> list:
        return self._sub_list

    def qsize(self) -> str:
        response = {}
        for connector in self.__connectors:
            response.update(connector.qsize())
        return json.dumps(response, indent=2)

    def is_empty(self, sub_list: list) -> str:
        response = {}
        for sub in sub_list:
            connector = self.__get_connector(sub)
            response[sub] = connector.is_empty(sub)
        return json.dumps(response, indent=2)

    def purge(self, sub_list: list) -> None:
        for sub in sub_list:
            connector = self._get_connector(sub)
            connector.purge(sub)

    def pull(self, sub_list: list, max_num: int, ack: bool = True) -> list:
        response = []
        connector: BaseSub

        for sub in sub_list:
            connector = self._get_connector(sub)
            response = connector.pull(sub=sub, max_num=max_num, ack=ack)
            if response != []:
                break

        return response

    def pull_nack(self, sub_list: list, max_num: int) -> list:
        return self.pull(sub_list=sub_list, max_num=max_num, ack=False)

    def _get_connector(self, sub_path: str) -> BaseSub:
        if re.search(
            r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
            sub_path
        ):
            connector = self._aws_sub
        elif re.search(
            r'projects/[a-z0-9-]+/subscriptions/.+',
            sub_path
        ):
            connector = self._gcp_sub
        else:
            raise ValueError(f'Invalid subscription: {sub_path}')

        return connector
