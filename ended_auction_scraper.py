import requests
import nbt
import io
import base64
import json
import logging
import os
import time
import sqlite3
from typing import Dict, Any, Tuple, List, Optional

# Configuration
API_URL = "https://api.hypixel.net/skyblock/auctions_ended"
API_KEY = os.getenv("HYPIXEL_API_KEY")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Setup logging
log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "auction_task.log")
DB_PATH = os.path.join(BASE_DIR, "auctions.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)

def nbt_to_dict(nbt_obj):
    """Recursively convert an NBT tag into a Python dict or value."""
    if hasattr(nbt_obj, 'tags'):  # TAG_Compound or TAG_List
        if isinstance(nbt_obj.tags, list):
            return {tag.name: nbt_to_dict(tag) for tag in nbt_obj.tags}
        else:
            return [nbt_to_dict(tag) for tag in nbt_obj.tags]
    elif hasattr(nbt_obj, 'value'):
        return nbt_obj.value
    else:
        return str(nbt_obj)

def decode_inventory_data(raw: str) -> Dict[str, Any]:
    """Decode base64 NBT item data from Hypixel auctions. Returns item attributes."""
    if not raw:
        logging.warning("Empty raw data provided to decode_inventory_data")
        return {"error": "Empty raw data"}

    try:
        nbt_data = base64.b64decode(raw)
        nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(nbt_data))

        if "i" not in nbt_file:
            return {"error": "No 'i' key in NBT data"}

        items = nbt_file["i"]
        if not items or len(items) == 0:
            return {"error": "No items in inventory"}

        item = items[0]
        result = {}
        if "Count" in item:
            result["count"] = item["Count"].value

        # If there are no ExtraAttributes, return basic info
        if "tag" not in item or "ExtraAttributes" not in item["tag"]:
            return result

        extra_attrs = item["tag"]["ExtraAttributes"]
        for k, v in extra_attrs.items():
            try:
                result[k] = nbt_to_dict(v)
            except Exception as e:
                logging.debug(f"Error extracting attribute {k}: {e}")
                result[k] = str(v)

        # SPECIAL: If this is a pet, update id to PET_<TYPE>
        if result.get("id") == "PET" and "petInfo" in result:
            try:
                pet_info = json.loads(result["petInfo"])
                pet_type = pet_info.get("type")
                if pet_type:
                    result["id"] = f"PET_{pet_type.upper()}"
            except Exception as e:
                logging.warning(f"Failed to extract petInfo type: {e}")

        return result

    except Exception as e:
        logging.error(f"Error decoding inventory data: {e}")
        return {"error": str(e)}

def fetch_auctions() -> Tuple[List[Dict], int]:
    headers = {}
    if API_KEY:
        headers["API-Key"] = API_KEY

    try:
        resp = requests.get(API_URL, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success", True):
            raise Exception(f"API returned error: {data.get('cause', 'Unknown error')}")

        auctions = data.get("auctions", [])
        last_updated = data.get("lastUpdated", int(time.time() * 1000))

        logging.info(f"Fetched {len(auctions)} auctions")
        return auctions, last_updated

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching auctions: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error fetching auctions: {e}")
        raise

def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute("""
            CREATE TABLE IF NOT EXISTS auctions (
                auction_id TEXT PRIMARY KEY,
                price REAL,
                timestamp INTEGER,
                bin BOOLEAN,
                id TEXT,
                item_attributes TEXT
            )
        """)

        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON auctions(timestamp)
        """)

        conn.commit()
        logging.info("Database initialized successfully")

    except Exception as e:
        logging.error(f"Error initializing database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def process_auction(auction_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Process a single auction's data and item attributes, making 'id' its own column."""
    try:
        auction_id = auction_data.get("auction_id")
        if not auction_id:
            logging.warning("Auction missing auction_id")
            return None

        item_bytes = auction_data.get("item_bytes", "")
        attrs = decode_inventory_data(item_bytes)

        if "error" in attrs:
            logging.warning(f"Failed to decode item for auction {auction_id}: {attrs['error']}")
            attrs = {}

        # Move id (item id) to top-level, keep rest in item_attributes
        item_id = attrs.pop("id", None)
        item_attributes = attrs  # All other attributes (except id)

        return {
            "auction_id": auction_id,
            "price": auction_data.get("price", 0),
            "timestamp": auction_data.get("timestamp", 0),
            "bin": bool(auction_data.get("bin", False)),
            "id": item_id,
            "item_attributes": json.dumps(item_attributes)
        }

    except Exception as e:
        logging.error(f"Error processing auction: {e}")
        return None

def save_auctions(processed_auctions: List[Dict[str, Any]]) -> int:
    """Insert processed auctions into the database."""
    if not processed_auctions:
        return 0

    saved_count = 0

    try:
        conn = sqlite3.connect(DB_PATH)

        for auction in processed_auctions:
            columns = ', '.join(f'"{col}"' for col in auction.keys())
            placeholders = ', '.join(['?'] * len(auction))
            values = list(auction.values())

            try:
                conn.execute(
                    f'INSERT INTO auctions ({columns}) VALUES ({placeholders})',
                    values
                )
                saved_count += 1
            except sqlite3.IntegrityError:
                logging.debug(f"Duplicate auction_id {auction['auction_id']}, skipping")
            except Exception as e:
                logging.error(f"Error saving auction {auction['auction_id']}: {e}")

        conn.commit()
        logging.info(f"Saved {saved_count} new auctions to database")

    except Exception as e:
        logging.error(f"Database error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

    return saved_count

def job():
    """Main job to fetch, process, and save auctions."""
    try:
        logging.info("Starting auction fetch job")
        auctions, last_updated = fetch_auctions()

        if not auctions:
            logging.info("No auctions fetched")
            return

        processed_auctions = []
        for auction in auctions:
            processed = process_auction(auction)
            if processed:
                processed_auctions.append(processed)

        saved_count = save_auctions(processed_auctions)

        logging.info(f"Job completed. Processed {len(processed_auctions)} auctions, "
                     f"saved {saved_count} new ones. Last updated: {last_updated}")

    except Exception as e:
        logging.error(f"Job failed: {e}", exc_info=True)

def main():
    logging.info("Starting Hypixel auction scraper")

    if not API_KEY:
        logging.warning("No HYPIXEL_API_KEY environment variable set. "
                        "You may hit rate limits without an API key.")

    init_db()

    while True:
        try:
            job()
            logging.info("Sleeping for 60 seconds...")
            time.sleep(60)
        except KeyboardInterrupt:
            logging.info("Received interrupt signal, shutting down...")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            logging.info("Sleeping for 60 seconds before retry...")
            time.sleep(60)

if __name__ == "__main__":
    main()