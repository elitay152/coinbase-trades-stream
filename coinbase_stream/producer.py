import asyncio
import json
import random
import time
from datetime import UTC, datetime

import websockets
from confluent_kafka import Producer
from websockets.exceptions import WebSocketException

from coinbase_stream.config import BOOTSTRAP_SERVERS, PRODUCTS, TOPIC

WS_URL = "wss://advanced-trade-ws.coinbase.com"

STALE_AFTER = 30 # seconds with no message
STABLE_AFTER = 60 # connection considered stable after this many seconds of messages
BACKOFF_BASE = 1
BACKOFF_MAX = 30

def on_delivery(err, msg):
    """Notification of delivery result on error."""
    if err is not None:
        print(f"delivery failed: {err}")

def flatten(msg, received_at):
    """Flatten the message to a single dict."""
    for event in msg.get("events", []):
        # trades will be listed newest first, send them oldest first
        for trade in reversed(event.get("trades", [])):
            yield {
                **trade,
                "event_type": event.get("type"),
                "exchange_ts": msg.get("timestamp"),
                "sequence_num": msg.get("sequence_num"),
                "received_at": received_at,
            }

def backoff_delay(attempt):
    """Exponential backoff with full jitter: random wait between 0 and the cap."""
    cap = min(BACKOFF_MAX, BACKOFF_BASE *2**attempt)
    return random.uniform(0, cap)

async def stream_trades(producer):
    """Connect once and send trades to Kafka until the connection fails."""
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "market_trades",
    }

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps(subscribe_msg))
        print("connected")

        while True:
            raw = await asyncio.wait_for(ws.recv(), timeout=STALE_AFTER)
            received_at = datetime.now(UTC).isoformat()
            msg = json.loads(raw)

            if msg.get("channel") != "market_trades":
                continue

            for trade in flatten(msg, received_at):
                producer.produce(
                    TOPIC,
                    key=trade["product_id"],
                    value=json.dumps(trade),
                    on_delivery=on_delivery,
                )
                print(trade["event_type"],
                      trade["product_id"],
                      trade["price"]
                      )

            producer.poll(0)

async def main(producer):
    attempt = 0

    while True:
        started = time.monotonic()
        try:
            await stream_trades(producer)
        except (WebSocketException, OSError, TimeoutError) as e:
            if time.monotonic() - started > STABLE_AFTER:
                attempt = 0

            delay = backoff_delay(attempt)
            print(f"connection lost ({type(e).__name__}: {e}); reconnecting in {delay:.1f}s")
            producer.poll(0)
            await asyncio.sleep(delay)
            attempt += 1

if __name__ == "__main__":

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    try:
        asyncio.run(main(producer))
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        producer.flush(10)