import asyncio
import json
from datetime import datetime, timezone

import websockets
from confluent_kafka import Producer

WS_URL = "wss://advanced-trade-ws.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD"]
TOPIC = "coinbase.trades"

producer = Producer({"bootstrap.servers": "localhost:19092"})

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

async def main():
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channel": "market_trades",
    }

    async with websockets.connect(WS_URL) as ws:
        await ws.send(json.dumps(subscribe_msg))

        async for raw in ws:
            received_at = datetime.now(timezone.utc).isoformat()
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

try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Stopping...")
finally:
    producer.flush(10)