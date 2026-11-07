import json

from coinbase_stream.producer import flatten
from coinbase_stream.sink import to_row
from tests.test_producer import SAMPLE

class FakeMessage:
    """Stands in for a Kafka message object."""

    def __init__(self, value, partition=0, offset=0):
        self._value = value
        self._partition = partition
        self._offset = offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    
def test_to_row_reads_producer_output():
    """to_row should return a tuple matching the INSERT_TRADE statement."""
    trade = next(flatten(SAMPLE, "2026-11-07T04:52:23.600000+00:00"))
    msg = FakeMessage(json.dumps(trade).encode(), partition=2, offset=42)

    row = to_row(msg)
    assert row[0] == "BTC-USD"
    assert row[1] == trade["trade_id"]
    assert row[2] == "75773.3"
    assert row[5] == trade["time"]
    assert row[-2:] == (2, 42)