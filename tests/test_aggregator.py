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

def test_duplicate_trade_counted_once():
    agg = Aggregator()
    agg.add(trade("1", "04:52:10", "100", size="2"), 0, 1)
    agg.add(trade("1", "04:52:10", "100", size="2"), 0, 5)  # e.g. resent in a reconnect snapshot
    agg.add(trade("9", "04:53:10", "105"), 0, 9)

    [candle] = agg.pop_closed()

    assert candle.volume == Decimal(2)
    assert len(candle.trade_ids) == 1
    assert agg.duplicates == 1

def test_window_stays_open_within_lateness():
    agg = Aggregator()
    agg.add(trade("1", "04:52:10", "100"), 0, 1)
    agg.add(trade("2", "04:53:03", "101"), 0, 2)  # watermark 04:52:58, before the window ends

    assert agg.pop_closed() == []

    agg.add(trade("3", "04:53:06", "102"), 0, 3)  # watermark 04:53:01, past the window end

    [candle] = agg.pop_closed()
    assert candle.window_start.minute == 52

def test_late_trade_is_dropped():
    agg = Aggregator()
    agg.add(trade("1", "04:52:10", "100"), 0, 1)
    agg.add(trade("9", "04:53:10", "105"), 0, 9)
    agg.pop_closed()
    watermark = agg.watermarks["BTC-USD"]

    agg.add(trade("2", "04:52:40", "999"), 0, 10)  # its window was already written

    assert agg.late == 1
    assert len(agg.candles) == 1  # only the open 04:53 candle
    assert agg.watermarks["BTC-USD"] == watermark

def test_safe_offsets_holds_back_for_open_candles():
    agg = Aggregator()
    agg.add(trade("1", "04:52:10", "100"), 0, 5)  # open candle starting at offset 5

    assert agg.safe_offsets({0: 10, 2: 20}) == {0: 5, 2: 20}