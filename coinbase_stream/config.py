import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:19092")
TOPIC = os.environ.get("TRADES_TOPIC", "coinbase.trades")
PRODUCTS = os.environ.get("PRODUCTS", "BTC-USD,ETH-USD,SOL-USD").split(",")

DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
TRADES_DB = DATA_DIR / "trades.duckdb"

POSTGRES_DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PORT', '5433')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'coinbase')} "
    f"user={os.environ.get('POSTGRES_USER', 'coinbase')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', '')}"
)