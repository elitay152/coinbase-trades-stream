from coinbase_stream import producer
from coinbase_stream.producer import BACKOFF_MAX, backoff_delay, flatten

SAMPLE = {
    "channel": "market_trades",
    "timestamp": "2026-11-07T04:52:23.526050702Z",
    "sequence_num": 4,
    "events": [
        {
            "type": "update",
            "trades": [
                {"product_id": "BTC-USD",
                 "trade_id": "1116684325",
                 "price": "75773.3",
                 "size": "0.00002015",
                 "time": "2026-11-07T04:52:23.470335Z",
                 "side": "SELL"},
                 {"product_id": "BTC-USD",
                  "trade_id": "1116684324",
                  "price": "75773.3",
                  "size": "0.00018519",
                  "time": "2026-11-07T04:52:23.470335Z",
                  "side": "SELL"},
            ],

        }
    ],
}

def test_one_record_per_trade():
    """Flatten should yield one record per trade."""
    trades = list(flatten(SAMPLE, "2026-11-07T04:52:23.600000+00:00"))
    assert len(trades) == 2

def test_trades_come_out_oldest_first():
    """Flatten should yield trades from oldest to newest."""
    trades = list(flatten(SAMPLE, "2026-11-07T04:52:23.600000+00:00"))
    assert [t["trade_id"] for t in trades] == ["1116684324", "1116684325"]

def test_adds_metadata_fields():
    """Flatten should add metadata fields to each trade."""
    trades = list(flatten(SAMPLE, "2026-11-07T04:52:23.600000+00:00"))
    for t in trades:
        assert t["event_type"] == "update"
        assert t["exchange_ts"] == "2026-11-07T04:52:23.526050702Z"
        assert t["sequence_num"] == 4
        assert t["received_at"] == "2026-11-07T04:52:23.600000+00:00"

def test_message_without_events_yields_nothing():
    """Flatten should yield nothing for a subscriptions-style message with no events."""
    trades = list(flatten({"channel": "market_trades"}, "2026-11-07T04:52:23.600000+00:00"))
    assert len(trades) == 0

def test_backoff_stays_within_cap():
    """backoff_delay should return a value between 0 and the cap."""
    for attempt in range(5):
        cap = 2**attempt

    for _ in range(1000):
        delay = backoff_delay(attempt)
        assert 0 <= delay <= cap

def test_backoff_never_exceeds_max():
    """backoff_delay should never return a value greater than BACKOFF_MAX."""
    for _ in range(1000):
        assert backoff_delay(20) <= BACKOFF_MAX

def test_on_delivery_counts_failures(monkeypatch):
    monkeypatch.setattr(producer, "delivery_failures", 0)
    producer.on_delivery(None, object())
    assert producer.delivery_failures == 0
    producer.on_delivery("broker down", object())
    assert producer.delivery_failures == 1