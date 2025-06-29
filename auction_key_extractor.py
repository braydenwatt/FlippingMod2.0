import requests
import nbt
import io
import base64
import json
import time

API_URL = "https://api.hypixel.net/skyblock/auctions_ended"

def decode_inventory_data(raw):
    try:
        nbt_file = nbt.nbt.NBTFile(fileobj=io.BytesIO(base64.b64decode(raw)))
        item = nbt_file["i"][0]
        extra_attrs = item["tag"]["ExtraAttributes"]

        result = {k: v.value for k, v in extra_attrs.items()}

        return result
    except Exception as e:
        return {"error": str(e)}

def fetch_auctions():
    resp = requests.get(API_URL)
    if resp.status_code != 200:
        raise Exception(f"HTTP {resp.status_code}: {resp.text}")
    data = resp.json()
    return data["auctions"], data["lastUpdated"]

def process_auctions(auctions):
    results = []
    for auc in auctions:
        attrs = decode_inventory_data(auc["item_bytes"])
        item_data = {
            "auction_id": auc["auction_id"],
            "price": auc["price"],
            "timestamp": auc["timestamp"],
            "bin": auc.get("bin", False),
            "item_attributes": attrs
        }
        results.append(item_data)
    return results

if __name__ == "__main__":
    auctions, last_updated = fetch_auctions()
    parsed = process_auctions(auctions)
    out_name = f"auctions_ended_{int(time.time())}.jsonl"
    with open(out_name, "w") as out:
        for item in parsed:
            out.write(json.dumps(item) + "\n")
    print(f"Fetched {len(parsed)} auctions. Saved to {out_name}. Last updated: {last_updated}")