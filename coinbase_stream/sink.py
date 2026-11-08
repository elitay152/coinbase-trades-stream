import json
import os

import duckdb
from confluent_kafka import Consumer

from coinbase_stream.config import BOOTSTRAP_SERVERS, TOPIC, TRADES_DB

GROUP_ID = "trades-sink"
BATCH_SIZE = 500
BATCH_TIMEOUT = 1.0
CRASH_AFTER_WRITE = os.environ.get("CRASH_AFTER_WRITE") == "1"

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    product_id VARCHAR NOT NULL,
    trade_id VARCHAR NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    size DECIMAL(18, 8) NOT NULL,
    side VARCHAR,
    trade_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR,
    exchange_ts TIMESTAMPTZ,
    sequence_num BIGINT,
    received_at TIMESTAMPTZ,
    consumed_at TIMESTAMPTZ DEFAULT current_timestamp,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    PRIMARY KEY (product_id, trade_id)
)
"""

INSERT_TRADE = """
INSERT INTO trades(
    product_id,
    trade_id,
    price,
    size,
    side,
    trade_time,
    event_type,
    exchange_ts,
    sequence_num,
    received_at,
    kafka_partition,
    kafka_offset
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO NOTHING
"""

def to_row(msg):
    """Turn a Kafka message into a tuple matching INSERT_TRADE."""
    trade = json.loads(msg.value())
    return (
        trade["product_id"],
        trade["trade_id"],
        trade["price"],
        trade["size"],
        trade.get("side"),
        trade["time"],
        trade.get("event_type"),
        trade.get("exchange_ts"),
        trade.get("sequence_num"),
        trade.get("received_at"),
        msg.partition(),
        msg.offset(),
    )

def main():
    TRADES_DB.parent.mkdir(exist_ok=True)
    con = duckdb.connect(str(TRADES_DB))
    con.execute(CREATE_TABLE)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE,
                                    timeout=BATCH_TIMEOUT)

            rows = []
            for msg in msgs:
                if msg.error():
                    print(f"consumer error: {msg.error()}")
                    continue
                rows.append(to_row(msg))

            if not rows:
                continue

            before = con.execute("select count(*) from trades").fetchone()[0]
            con.begin()
            con.executemany(INSERT_TRADE, rows)
            con.commit()
            after = con.execute("select count(*) from trades").fetchone()[0]
            if CRASH_AFTER_WRITE:
                print("simulating crash: rows written, offsets NOT committed")
                os._exit(1)

            # only after the rows are safely in DuckDB
            consumer.commit(asynchronous=False)
            print(f"wrote batch of {len(rows)} ({after - before} new)")
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        consumer.close()
        con.close()

if __name__ == "__main__":
    main()