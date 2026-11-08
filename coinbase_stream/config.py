import os
from pathlib import Path

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.environ.get("TRADES_TOPIC", "coinbase.trades")
PRODUCTS = os.environ.get("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
TRADES_DB = DATA_DIR / "trades.duckdb"
CANDLES_DB = DATA_DIR / "candles.duckdb"