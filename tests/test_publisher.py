import pytest
from queuing_hub.conn.aws import AwsPub
from queuing_hub.conn.gcp import GcpPub

class TestPublisher:

    def test_get_connector_aws_ok(self, pub):
        topic_aws_ok = 'https://test-test-test.queue.amazonaws.com/1234567890/test-queue'
        conn_aws_ok = pub._get_connector(topic=topic_aws_ok)

        assert type(conn_aws_ok) == AwsPub

    def test_get_connector_aws_ng(self, pub):
        topic_aws_ng = 'https://test-test-test.queue.amazon.com/1234567890/test-queue'
        with pytest.raises(ValueError) as exception:
            _ = pub._get_connector(topic=topic_aws_ng)

        assert str(exception.value) == f'Invalid topic: {topic_aws_ng}'