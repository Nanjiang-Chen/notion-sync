import os
import time
import requests
from datetime import datetime, timezone

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
CRYPTO_DB_ID = os.environ["NOTION_CRYPTO_DB_ID"]
ETF_DB_ID = os.environ["NOTION_ETF_DB_ID"]

VS_CURRENCY = "aud"
COINS = {
    "Bitcoin": "bitcoin",
    "WLFI": "world-liberty-financial",
}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def coingecko_get_prices(coin_ids, vs=VS_CURRENCY):
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ",".join(coin_ids), "vs_currencies": vs}
    headers = {"User-Agent": "notion-crypto-sync/1.0"}

    for attempt in range(6):
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.status_code == 429:
            sleep_s = 2 * (attempt + 1)
            print(f"[CoinGecko] 429 rate limited, sleep {sleep_s}s...")
            time.sleep(sleep_s)
            continue
        r.raise_for_status()
        return r.json()

    raise RuntimeError("CoinGecko rate limited too many times (429). Try lower frequency.")

def notion_query_page_id_by_name(name: str) -> str:
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    payload = {
        "filter": {
            "property": "Name",
            "title": {"equals": name}
        }
    }
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=20)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        raise RuntimeError(f'Notion 中找不到 Name == "{name}" 的行（注意大小写/空格）')
    return results[0]["id"]

def notion_get_database_properties():
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=20)
    r.raise_for_status()
    return r.json().get("properties", {})

def notion_update_price(page_id: str, price: float, has_last_updated: bool):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    props = {"Current Price": {"number": price}}
    if has_last_updated:
        props["Last Updated"] = {"date": {"start": datetime.now(timezone.utc).isoformat()}}
    payload = {"properties": props}
    r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=20)
    r.raise_for_status()

def main():
    db_props = notion_get_database_properties()
    has_last_updated = "Last Updated" in db_props
    print("[Notion] Last Updated:", "YES" if has_last_updated else "NO (skip)")

    prices = coingecko_get_prices(list(COINS.values()), VS_CURRENCY)

    for notion_name, cg_id in COINS.items():
        price = float(prices[cg_id][VS_CURRENCY])
        page_id = notion_query_page_id_by_name(notion_name)
        notion_update_price(page_id, price, has_last_updated)
        print(f"✅ Updated {notion_name}: {price} {VS_CURRENCY.upper()}")

    print("🎉 Done.")

if __name__ == "__main__":
    main()
