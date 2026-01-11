import requests
import pandas as pd
import concurrent.futures
import time

# --- CONFIG ---
BASE_URL = "https://api.backpack.exchange/api/v1"


def get_json(url, params=None, retries=3):
    """Safe request with retry and error logging"""
    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)

            # Якщо ліміт перевищено (429), чекаємо і пробуємо знову
            if response.status_code == 429:
                print(f"⚠️ Rate limit hit for {params.get('symbol', 'data')}, pausing...")
                time.sleep(2)
                continue

            response.raise_for_status()
            return response.json()
        except Exception as e:
            if i == retries - 1:
                # Виводимо помилку тільки після останньої спроби
                print(f"❌ Failed {url} params={params}: {e}")
                return None
            time.sleep(0.5)


def fetch_pair_details(symbol):
    """Fetches Depth (Bid/Ask) and Funding for one symbol"""

    # 1. DEPTH (BID/ASK)
    # Зменшуємо навантаження, запитуючи лише 1 рівень стакану
    depth = get_json(f"{BASE_URL}/depth", params={'symbol': symbol, 'limit': 1})

    bid, ask = 0.0, 0.0
    if depth:
        try:
            if depth.get('bids'):
                bid = float(depth['bids'][0][0])
            if depth.get('asks'):
                ask = float(depth['asks'][0][0])
        except (IndexError, ValueError) as e:
            print(f"⚠️ Parse error for {symbol}: {e}")

    # 2. FUNDING RATE
    funding_res = get_json(f"{BASE_URL}/fundingRates", params={'symbol': symbol})
    funding = 0.0
    if funding_res and isinstance(funding_res, list) and len(funding_res) > 0:
        funding = float(funding_res[0].get('fundingRate', 0)) * 100

    # Пауза, щоб не "злити" API
    time.sleep(0.1)

    return symbol, bid, ask, funding


def main():
    print("🚀 Fetching Backpack Data (Safe Mode)...")
    start_time = time.time()

    # 1. Загальні запити (Markets, Tickers, OpenInterest)
    markets = get_json(f"{BASE_URL}/markets")
    tickers = get_json(f"{BASE_URL}/tickers")
    oi_data = get_json(f"{BASE_URL}/openInterest")

    if not markets or not tickers:
        print("❌ Critical: Failed to fetch base market data.")
        return

    # Фільтруємо тільки PERP
    perp_symbols = [m['symbol'] for m in markets if m.get('marketType') == 'PERP']
    print(f"📋 Found {len(perp_symbols)} perps. Fetching detailed data...")

    # Створюємо мапи для швидкого пошуку
    ticker_map = {t['symbol']: t for t in tickers}
    oi_map = {o['symbol']: o['openInterest'] for o in oi_data} if oi_data else {}

    # 2. Паралельне завантаження (Зменшено кількість потоків до 4)
    detailed_data = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        results = executor.map(fetch_pair_details, perp_symbols)

        for symbol, bid, ask, funding in results:
            detailed_data[symbol] = {'bid': bid, 'ask': ask, 'funding': funding}

    # 3. Зведення даних
    final_rows = []
    for symbol in perp_symbols:
        t_data = ticker_map.get(symbol, {})
        d_data = detailed_data.get(symbol, {})

        last_price = float(t_data.get('lastPrice', 0))
        vol_usd = float(t_data.get('quoteVolume', 0))
        oi_usd = float(oi_map.get(symbol, 0)) * last_price  # OI in USD

        final_rows.append({
            "SYMBOL": symbol,
            "BID": d_data.get('bid', 0),
            "ASK": d_data.get('ask', 0),
            "PRICE": last_price,
            "FUNDING %": d_data.get('funding', 0),
            "VOL 24h ($)": vol_usd,
            "OI ($)": oi_usd
        })

    # 4. Вивід таблиці
    df = pd.DataFrame(final_rows)
    df = df.sort_values(by="VOL 24h ($)", ascending=False)

    # Форматування
    pd.options.display.float_format = '{:,.4f}'.format
    pd.options.display.max_columns = 10
    pd.options.display.width = 1000

    elapsed = time.time() - start_time
    print(f"\n✅ Fetched {len(df)} pairs in {elapsed:.2f}s")
    print("-" * 100)
    print(df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()