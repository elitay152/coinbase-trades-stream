import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import psycopg
import pytest

from coinbase_stream.aggregator import (
    CREATE_TABLE,
    WINDOW,
    Aggregator,
    Candle,
    load_watermarks,
    write_candles,
)
from coinbase_stream.config import POSTGRES_DSN

T0 = datetime(2026, 11, 7, 12, 0, tzinfo=UTC)

@pytest.fixture
def con():
    try:
        con = psycopg.connect(POSTGRES_DSN, connect_timeout=3)
    except psycopg.OperationalError:
        if os.environ.get("REQUIRE_POSTGRES") == "1":
            raise
        pytest.skip("Postgres is not running")

    # separate test schema
    con.execute("DROP SCHEMA IF EXISTS test_aggregator CASCADE")
    con.execute("CREATE SCHEMA test_aggregator")
    con.execute("SET search_path TO test_aggregator")
    con.execute(CREATE_TABLE)
    con.commit()

    yield con

    con.rollback()
    con.execute("DROP SCHEMA test_aggregator CASCADE")
    con.commit()
    con.close()

def make_candle(window_start, price):
    key = (window_start, 1)
    candle = Candle.start("BTC-USD", window_start, 0, 0, key, Decimal(price))
    candle.add("1", key, Decimal(price), Decimal("0.5"), 0)
    return candle

def test_rewriting_a_candle_updates_it(con):
    write_candles(con, [make_candle(T0, "100")])
    write_candles(con, [make_candle(T0, "101")]) # e.g. after a replay

    rows = con.execute("SELECT close, trade_count FROM candles").fetchall()
    assert rows == [(Decimal("101.00000000"), 1)]

def test_load_watermarks_returns_datetimes(con):
    write_candles(con, [make_candle(T0, "100"), make_candle(T0 + timedelta(minutes=1), "101")])

    agg = Aggregator()
    load_watermarks(con, agg)

    watermark = agg.watermarks["BTC-USD"]
    assert isinstance(watermark, datetime)
    assert watermark == T0 + timedelta(minutes=1) + WINDOW