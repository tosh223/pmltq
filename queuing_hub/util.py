import re

from queuing_hub.conn.base import BaseSub
from queuing_hub.conn.aws import AwsSub
from queuing_hub.conn.gcp import GcpSub

def get_connector(sub_path: str) -> BaseSub:
    if re.search(
        r'https://.+-.+-.+\.queue\.amazonaws\.com/[0-9]+/.+',
        sub_path
    ):
        connector = AwsSub()
    elif re.search(
        r'projects/[a-z0-9-]+/(topics|subscriptions)/.+',
        sub_path
    ):
        connector = GcpSub()
    else:
        raise ValueError(f'invalid subscription: {sub_path}')
    
    return connector
