import requests
import nbt
import io
import base64
import json
import logging
import os
import time
import sqlite3

API_URL = "https://api.hypixel.net/skyblock/auctions_ended"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "auction_task.log")
DB_PATH = os.path.join(BASE_DIR, "auctions.db")

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def decode_inventory_data(raw):
    def convert(obj):
        if isinstance(obj, (bytes, bytearray)):
            try:
                return obj.decode('utf-8', errors='replace')
            except Exception:
                return obj.hex()
        elif isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(v) for v in obj]
        else:
            return obj
    try:
        nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(base64.b64decode(raw)))
        item = nbt_file["i"][0]
        extra_attrs = item["tag"]["ExtraAttributes"]
        result = {k: v.value for k, v in extra_attrs.items()}
        return convert(result)
    except Exception as e:
        return {"error": str(e)}

def fetch_auctions():
    resp = requests.get(API_URL)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    data = resp.json()
    return data["auctions"], data["lastUpdated"]

def init_db():
    with sqlite3.connect(DB_PATH, timeout=30) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS auctions (
                auction_id TEXT PRIMARY KEY,
                price REAL,
                timestamp INTEGER,
                bin BOOLEAN,
                item_attributes TEXT
            )
        """)
        conn.commit()

def job():
    try:
        auctions, last_updated = fetch_auctions()
        new_count = 0
        with sqlite3.connect(DB_PATH, timeout=30) as conn:
            c = conn.cursor()
            for auc in auctions:
                auction_id = auc["auction_id"]
                attrs = decode_inventory_data(auc["item_bytes"])
                try:
                    c.execute(
                        "INSERT INTO auctions (auction_id, price, timestamp, bin, item_attributes) VALUES (?, ?, ?, ?, ?)",
                        (auction_id, auc["price"], auc["timestamp"], int(auc.get("bin", False)), json.dumps(attrs))
                    )
                    new_count += 1
                except sqlite3.IntegrityError:
                    continue
            conn.commit()
        logging.info(f"Added {new_count} new auctions. Last updated: {last_updated}")
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)

if __name__ == "__main__":
    init_db()
    while True:
        job()
        time.sleep(60)