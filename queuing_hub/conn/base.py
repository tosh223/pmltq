class BasePub():
    def __init__(self):
        pass

    def push(self, topic: str, body: str) -> None:
        pass


class BaseSub():
    def __init__(self):
        pass

    def qsize(self, sub_list: list = None) -> dict:
        pass

    def is_empty(self, sub: str) -> bool:
        pass

    def purge(self, sub: str) -> None:
        pass

    def pull(self, sub: str, max_num: int = 1, ack: bool = False) -> list:
        pass

    def _ack(self, sub: str, messages: list) -> None:
        pass
