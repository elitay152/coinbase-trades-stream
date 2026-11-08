from decimal import Decimal

from coinbase_stream.aggregator import Aggregator


def trade(trade_id, hms, price, size="1"):
    return {
        "product_id": "BTC-USD",
        "trade_id": trade_id,
        "price": price,
        "size": size,
        "time": f"2026-11-07T{hms}.000000Z",
    }

def test_open_and_close_use_event_time_not_arrival_order():
    agg = Aggregator()
    agg.add(trade("3", "04:52:30", "102"), 0, 3)
    agg.add(trade("1", "04:52:10", "100"), 0, 1)
    agg.add(trade("2", "04:52:20", "101"), 0, 2)
    agg.add(trade("9", "04:53:10", "105"), 0, 9)  # moves the watermark past 04:53:00

    [candle] = agg.pop_closed()

    assert candle.open == Decimal(100)
    assert candle.close == Decimal(102)
    assert len(candle.trade_ids) == 3

def test_volume_and_vwap():
    agg = Aggregator()
    agg.add(trade("1", "04:52:10", "100", size="1"), 0, 1)
    agg.add(trade("2", "04:52:20", "200", size="3"), 0, 2)
    agg.add(trade("9", "04:53:10", "150"), 0, 0)

    [candle] = agg.pop_closed()
    row = candle.row()

    assert candle.volume == Decimal(4)
    assert Decimal(row[7]) == Decimal(175) # (100*1 + 200*3) / 4