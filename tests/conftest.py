import pytest
from queuing_hub.publisher import Publisher

@pytest.fixture(scope="session")
def pub():
    return Publisher()