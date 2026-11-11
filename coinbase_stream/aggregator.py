import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer, TopicPartition

from coinbase_stream.config import BOOTSTRAP_SERVERS, POSTGRES_DSN, TOPIC

GROUP_ID = "trades-aggregator"
BATCH_SIZE = 500
BATCH_TIMEOUT = 1.0
WINDOW = timedelta(minutes=1)
ALLOWED_LATENESS = timedelta(seconds=5)
EIGHT_PLACES = Decimal("0.00000001")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS candles (
    product_id VARCHAR NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    open DECIMAL(18, 8) NOT NULL,
    high DECIMAL(18, 8) NOT NULL,
    low DECIMAL(18, 8) NOT NULL,
    close DECIMAL(18, 8) NOT NULL,
    volume DECIMAL(24, 8) NOT NULL,
    vwap DECIMAL(18, 8) NOT NULL,
    trade_count INTEGER NOT NULL,
    emitted_at TIMESTAMPTZ DEFAULT current_timestamp,
    PRIMARY KEY (product_id, window_start)
)
"""

UPSERT_CANDLE = """
INSERT INTO candles (
    product_id, window_start, open, high, low, close, volume, vwap, trade_count
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (product_id, window_start) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    vwap = EXCLUDED.vwap,
    trade_count = EXCLUDED.trade_count,
    emitted_at = now()
"""

@dataclass
class Candle:
    product_id: str
    window_start: datetime
    partition: int
    min_offset: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    open_key: tuple
    close_key: tuple
    volume: Decimal = Decimal(0)
    notional: Decimal = Decimal(0) # sum of price * size for VWAP
    trade_ids: set = field(default_factory=set)

    @classmethod
    def start(cls, product_id, window_start, partition, offset, key, price):
        return cls(
            product_id,
            window_start,
            partition,
            offset,
            open=price,
            high=price,
            low=price,
            close=price,
            open_key=key,
            close_key=key,
        )

    @property
    def window_end(self):
        return self.window_start + WINDOW

    def add(self, trade_id, key, price, size, offset):
        """Add a trade; return False if it was already counted."""
        if trade_id in self.trade_ids:
            return False
        self.trade_ids.add(trade_id)

        # open/close follow event time, not arrival order
        if key < self.open_key:
            self.open_key, self.open = key, price
        if key > self.close_key:
            self.close_key, self.close = key, price

        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.volume += size
        self.notional += price * size
        self.min_offset = min(self.min_offset, offset)
        return True

    def row(self):
        vwap = (self.notional / self.volume).quantize(EIGHT_PLACES) if self.volume else self.close
        return (
            self.product_id,
            self.window_start,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            vwap,
            len(self.trade_ids),
        )

class Aggregator:
    def __init__(self):
        self.candles = {} # (product_id, window_start) -> Candle
        self.watermarks = {} # product_id -> datetime
        self.late = 0
        self.duplicates = 0

    def add(self, trade, partition, offset):
        product = trade["product_id"]
        time = datetime.fromisoformat(trade["time"])
        window_start = time.replace(second=0, microsecond=0)
        watermark = self.watermarks.get(product)

        if watermark is not None and window_start + WINDOW <= watermark:
            self.late += 1
            return

        price = Decimal(trade["price"])
        size = Decimal(trade["size"])
        key = (time, int(trade["trade_id"])) # trade_id breaks ties at same timestamp

        candle = self.candles.get((product, window_start))
        if candle is None:
            candle = Candle.start(product, window_start, partition, offset, key, price)
            self.candles[(product, window_start)] = candle

        if not candle.add(trade["trade_id"], key, price, size, offset):
            self.duplicates += 1
            return

        new_watermark = time - ALLOWED_LATENESS
        if watermark is None or new_watermark > watermark:
            self.watermarks[product] = new_watermark

    def pop_closed(self):
        """Remove and return candles the watermark has passed, oldest first."""
        closed = [
            key for key, c in self.candles.items() if c.window_end <= self.watermarks[c.product_id]
        ]
        return [self.candles.pop(key) for key in sorted(closed, key=lambda k: k[1])]

    def safe_offsets(self, consumed):
        """Offsets we can commit without losing trades that are still in open candles."""
        safe = dict(consumed)
        for c in self.candles.values():
            if c.partition in safe:
                safe[c.partition] = min(safe[c.partition], c.min_offset)
        return safe

def load_watermarks(con, agg):
    """After a restart, skip windows that were already written."""
    rows = con.execute(
        "SELECT product_id, max(window_start) FROM candles GROUP BY product_id"
    ).fetchall()

    for product, last_start in rows:
        agg.watermarks[product] = last_start + WINDOW

def write_candles(con, candles):
    """Upsert closed candles in one transaction."""
    with con.cursor() as cur:
        cur.executemany(UPSERT_CANDLE, [c.row() for c in candles])
    con.commit()

def main():
    con = psycopg.connect(POSTGRES_DSN)
    con.execute(CREATE_TABLE)
    con.commit()

    agg = Aggregator()
    load_watermarks(con, agg)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    consumed = {} # partition -> next offset to read

    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE,
                                    timeout=BATCH_TIMEOUT)

            for msg in msgs:
                if msg.error():
                    print(f"consumer error: {msg.error()}")
                    continue
                agg.add(json.loads(msg.value()),
                        msg.partition(),
                        msg.offset())
                consumed[msg.partition()] = msg.offset() + 1

            closed = agg.pop_closed()
            if closed:
                write_candles(con, closed)
                for c in closed:
                    print(
                        f"{c.product_id} {c.window_start:%H:%M} "
                        f"O={c.open} H={c.high} L={c.low} C={c.close} "
                        f"V={c.volume} n={len(c.trade_ids)} "
                        f"(late={agg.late} dupes={agg.duplicates})"
                        )
            if msgs or closed:
                offsets = [
                    TopicPartition(TOPIC, partition, offset)
                    for partition, offset in agg.safe_offsets(consumed).items()
                ]
                if offsets:
                    consumer.commit(offsets=offsets, asynchronous=False)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
        con.close()

if __name__ == "__main__":
    main()