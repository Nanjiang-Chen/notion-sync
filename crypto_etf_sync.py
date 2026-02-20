import os
import time
import requests
from datetime import datetime, timezone
from typing import Dict, Optional

# ===== Notion =====
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
CRYPTO_DB_ID = os.environ["NOTION_CRYPTO_DB_ID"]
ETF_DB_ID = os.environ["NOTION_ETF_DB_ID"]

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ===== What to sync =====
VS_CURRENCY = "aud"

# Notion Name (Title) -> CoinGecko coin id
CRYPTO = {
    "Bitcoin": "bitcoin",
    "WLFI": "world-liberty-financial",
}

# Notion Name (Title) -> Yahoo ticker
ETFS = {
    "IVV": "IVV.AX",
    "AGS": "AGS.AX",
}

# Column names in both databases
TITLE_PROP = "Name"           # 你的 Title 列叫 Name
PRICE_PROP = "Current Price"  # 你用的是 Current Price（Number）
LAST_UPDATED_PROP = "Last Updated"  # 可选：没有就跳过


# ---------------- CoinGecko ----------------
def coingecko_get_prices(coin_ids, vs=VS_CURRENCY) -> Dict[str, Dict[str, float]]:
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": ",".join(coin_ids), "vs_currencies": vs}
    headers = {"User-Agent": "notion-sync/1.0"}

    for attempt in range(6):
        r = requests.get(url, params=params, headers=headers, timeout=20)
        if r.status_code == 429:
            sleep_s = 2 * (attempt + 1)
            print(f"[CoinGecko] 429 rate limited, sleep {sleep_s}s...")
            time.sleep(sleep_s)
            continue
        r.raise_for_status()
        return r.json()

    raise RuntimeError("CoinGecko rate limited too many times (429).")


# ---------------- Yahoo Finance (ASX ETFs) ----------------
def yahoo_last_price(ticker: str) -> Optional[float]:
    """
    ticker: e.g. 'IVV.AX'
    Returns regularMarketPrice (AUD) if available.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"interval": "1m", "range": "1d", "includePrePost": "false"}
    headers = {"User-Agent": "notion-sync/1.0"}

    r = requests.get(url, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    data = r.json()

    result = data.get("chart", {}).get("result")
    err = data.get("chart", {}).get("error")
    if err or not result:
        return None

    meta = result[0].get("meta", {})
    price = meta.get("regularMarketPrice") or meta.get("previousClose")
    return float(price) if price is not None else None


# ---------------- Notion helpers ----------------
def notion_get_database_properties(database_id: str) -> Dict:
    url = f"https://api.notion.com/v1/databases/{database_id}"
    r = requests.get(url, headers=NOTION_HEADERS, timeout=20)
    r.raise_for_status()
    return r.json().get("properties", {})

def notion_find_page_id_by_title(database_id: str, title_value: str) -> str:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    payload = {
        "filter": {
            "property": TITLE_PROP,
            "title": {"equals": title_value}
        }
    }
    r = requests.post(url, headers=NOTION_HEADERS, json=payload, timeout=20)
    r.raise_for_status()
    results = r.json().get("results", [])
    if not results:
        raise RuntimeError(f'Notion DB({database_id}) 找不到 {TITLE_PROP} == "{title_value}" 的行')
    return results[0]["id"]

def notion_update_price(database_props: Dict, page_id: str, price: float):
    # 校验列存在（避免 400）
    if PRICE_PROP not in database_props:
        raise RuntimeError(f"Notion 数据库缺少列: {PRICE_PROP}")

    props_payload = {PRICE_PROP: {"number": price}}

    if LAST_UPDATED_PROP in database_props:
        props_payload[LAST_UPDATED_PROP] = {
            "date": {"start": datetime.now(timezone.utc).isoformat()}
        }

    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {"properties": props_payload}
    r = requests.patch(url, headers=NOTION_HEADERS, json=payload, timeout=20)
    if r.status_code >= 400:
        print("[Notion] Update failed:", r.status_code, r.text)
    r.raise_for_status()


def sync_crypto():
    print("=== Sync Crypto ===")
    db_props = notion_get_database_properties(CRYPTO_DB_ID)

    prices = coingecko_get_prices(list(CRYPTO.values()), VS_CURRENCY)

    for notion_name, cg_id in CRYPTO.items():
        if cg_id not in prices or VS_CURRENCY not in prices[cg_id]:
            raise RuntimeError(f"CoinGecko 返回缺少 {cg_id}/{VS_CURRENCY}: {prices}")

        price = float(prices[cg_id][VS_CURRENCY])
        page_id = notion_find_page_id_by_title(CRYPTO_DB_ID, notion_name)
        notion_update_price(db_props, page_id, price)
        print(f"✅ Crypto Updated {notion_name}: {price} {VS_CURRENCY.upper()}")

def sync_etfs():
    print("=== Sync ETFs (ASX via Yahoo) ===")
    db_props = notion_get_database_properties(ETF_DB_ID)

    for notion_name, ticker in ETFS.items():
        price = yahoo_last_price(ticker)
        if price is None:
            raise RuntimeError(f"Yahoo 无法获取价格：{ticker}（可能代码不对或未收录）")

        page_id = notion_find_page_id_by_title(ETF_DB_ID, notion_name)
        notion_update_price(db_props, page_id, price)
        print(f"✅ ETF Updated {notion_name} ({ticker}): {price} AUD")

def main():
    sync_crypto()
    sync_etfs()
    print("🎉 All done.")

if __name__ == "__main__":
    main()
