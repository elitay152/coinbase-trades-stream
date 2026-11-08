import sys
from collections import Counter

from confluent_kafka import Consumer

from coinbase_stream.config import BOOTSTRAP_SERVERS, TOPIC


def main():
    name = sys.argv[1] if len(sys.argv) > 1 else "demo"

    def on_assign(consumer, partitions):
        print(f"[{name}] assigned: {[p.partition for p in partitions]}")

    def on_revoke(consumer, partitions):
        print(f"[{name}] revoked:  {[p.partition for p in partitions]}")

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "rebalance-demo",
        "client.id": name,
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    counts = Counter()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            counts[(msg.partition(), msg.key().decode())] += 1
            if sum(counts.values()) % 50 == 0:
                print(f"[{name}] {dict(counts)}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()