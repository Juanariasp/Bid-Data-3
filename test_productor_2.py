import pytest
from unittest.mock import MagicMock
from productor_2 import get_data

@pytest.fixture
def kinesis_client_mock():
    # Creamos un mock del cliente de Kinesis
    kinesis_client = MagicMock()
    yield kinesis_client

def test_get_data():
    data = get_data()

    assert 'event_time' in data
    assert 'stock' in data
    assert 'price' in data
    