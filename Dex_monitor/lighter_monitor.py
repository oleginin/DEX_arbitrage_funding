import requests
import pandas as pd
import time
import sqlite3
import os
import random
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

API_BASE = "https://mainnet.zklighter.elliot.ai/api/v1"
FUNDING_URL = "https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"

# ШЛЯХИ
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'lighter_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

UPDATE_INTERVAL_SLOW = 3600

# Заголовки (Stealth)
HEADERS = {
    "authority": "mainnet.zklighter.elliot.ai",
    "accept": "application/json, text/plain, */*",
    "origin": "https://lighter.xyz",
    "referer": "https://lighter.xyz/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

session = requests.Session()
session.headers.update(HEADERS)


class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


pd.set_option('display.max_rows', None)
pd.set_option('display.width', 250)
pd.set_option('display.float_format', '{:,.5f}'.format)


# ═══════════════════════════════════════════════════════════════════════════
# 🗄️ БАЗА ДАНИХ
# ═══════════════════════════════════════════════════════════════════════════

def init_db():
    if not os.path.exists(DB_FOLDER):
        try:
            os.makedirs(DB_FOLDER)
        except:
            pass

    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL;')
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            token TEXT PRIMARY KEY,
            bid REAL,
            ask REAL,
            spread_pct REAL,
            funding_pct REAL,
            freq_hours INTEGER,
            oi_usd REAL,
            volume_24h REAL,
            last_updated TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print(f"{C.GREEN}✅ DB Connected: {DB_PATH}{C.END}")


def save_to_db(data_list):
    """Зберігає список даних у базу"""
    if not data_list: return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        for row in data_list:
            # Використовуємо INSERT OR REPLACE для повного оновлення
            cursor.execute('''
                INSERT OR REPLACE INTO market_data 
                (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row['Token'], row['Bid'], row['Ask'], row['Spread %'],
                row['Funding %'],
                1,  # <--- 🔥 ЖОРСТКО 1 ГОДИНА 🔥
                row['OI ($)'],
                row['Volume 24h ($)'], timestamp
            ))
        conn.commit()
    except Exception as e:
        print(f"{C.RED}❌ DB Error: {e}{C.END}")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# 📡 API ФУНКЦІЇ
# ═══════════════════════════════════════════════════════════════════════════

def get_json(url, retries=2):
    for i in range(retries):
        try:
            response = session.get(url, timeout=10)

            if response.status_code == 429:
                wait = 60  # Чекаємо хвилину при бані
                print(f"{C.YELLOW}⚠️ Rate Limit (429). Waiting {wait}s...{C.END}")
                time.sleep(wait)
                continue

            if response.status_code != 200:
                return None
            return response.json()
        except Exception:
            time.sleep(1)
    return None


def get_active_markets():
    """Крок 1: Список маркетів"""
    url = f"{API_BASE}/orderBookDetails?filter=perp"
    data = get_json(url)

    markets = []
    if not data or 'order_book_details' not in data:
        return []

    for item in data['order_book_details']:
        if item.get('status') == 'active':
            vol_usd = float(item.get('daily_quote_token_volume', 0))
            if vol_usd > 500:  # Фільтр сміття
                markets.append({
                    'symbol': item.get('symbol'),
                    'market_id': item.get('market_id'),
                    'volume_usd': vol_usd,
                    'oi_tokens': float(item.get('open_interest', 0))
                })
    return markets


def fetch_all_funding_rates():
    """Крок 2: Bulk Funding"""
    data = get_json(FUNDING_URL)
    funding_map = {}

    if not data or 'funding_rates' not in data:
        return {}

    for item in data['funding_rates']:
        if item.get('exchange') != 'lighter':  # Фільтр по біржі
            continue
        mid = item.get('market_id')
        raw_rate = float(item.get('rate', 0))
        funding_map[mid] = raw_rate * 100.0  # BPS -> %

    return funding_map


def fetch_single_orderbook(market_info, funding_rate):
    """Крок 3: Отримання стакану (один запит)"""
    mid = market_info['market_id']
    symbol = market_info['symbol']

    book_url = f"{API_BASE}/orderBookOrders?market_id={mid}&limit=1"

    try:
        response = session.get(book_url, timeout=5)
        if response.status_code != 200: return None
        book_data = response.json()
    except:
        return None

    bid, ask = 0.0, 0.0
    if book_data:
        if book_data.get('total_bids', 0) > 0:
            bid = float(book_data['bids'][0]['price'])
        if book_data.get('total_asks', 0) > 0:
            ask = float(book_data['asks'][0]['price'])

    if bid == 0:
        return None

    spread = ((ask - bid) / bid) * 100
    oi_usd = market_info['oi_tokens'] * bid

    return {
        'Token': symbol,
        'Bid': bid,
        'Ask': ask,
        'Spread %': spread,
        'Funding %': funding_rate,
        'Freq (h)': 1,  # 1 година
        'OI ($)': oi_usd,
        'Volume 24h ($)': market_info['volume_usd']
    }


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 LIGHTER MONITOR (60 REQ/MIN LIMIT){C.END}")
    print(f"{C.YELLOW}📂 DB Path: {DB_PATH}{C.END}")

    init_db()

    last_slow_update = 0
    first_run = True
    markets_meta = []

    while True:
        try:
            current_time = time.time()

            # 1. ОНОВЛЮЄМО СПИСОК (Раз на годину)
            if (current_time - last_slow_update) >= UPDATE_INTERVAL_SLOW or not markets_meta:
                if first_run: print(f"{C.BOLD}🔄 Fetching Market List...{C.END}")
                new_meta = get_active_markets()
                if new_meta:
                    markets_meta = new_meta
                    last_slow_update = time.time()
                    if first_run: print(f"{C.GREEN}✅ Loaded {len(markets_meta)} markets.{C.END}")
                else:
                    time.sleep(10)
                    continue

            # 2. ОНОВЛЮЄМО ФАНДІНГ (1 запит)
            funding_map = fetch_all_funding_rates()
            if not funding_map:
                print(f"{C.YELLOW}⚠️ Funding API error. Waiting...{C.END}")
                time.sleep(10)
                continue

            # 3. ПРОХОДИМО ПО МАРКЕТАХ (Послідовно!)
            results = []
            if first_run: print(f"{C.BOLD}🔄 Scanning markets (1 per sec)...{C.END}")

            for i, m in enumerate(markets_meta):
                mid = m['market_id']
                rate = funding_map.get(mid, 0.0)

                # Запит
                res = fetch_single_orderbook(m, rate)
                if res: results.append(res)

                # 🔥 ГАЛЬМА (THROTTLE) 🔥
                # 1.1 секунди затримки гарантують < 60 запитів на хвилину
                time.sleep(1.1)

                # Вивід прогресу кожні 10 токенів
                if first_run and (i + 1) % 10 == 0:
                    print(f"   Processed {i + 1}/{len(markets_meta)}...")

            # 4. ЗБЕРЕЖЕННЯ
            if results:
                save_to_db(results)

                if first_run:
                    print("\n")
                    df = pd.DataFrame(results)
                    df = df.sort_values(by='Volume 24h ($)', ascending=False)
                    cols = ['Token', 'Bid', 'Ask', 'Spread %', 'Funding %', 'Freq (h)', 'OI ($)', 'Volume 24h ($)']
                    print("=" * 130)
                    print(f"{C.BOLD}📊 LIGHTER INITIAL DATA (Top 10){C.END}")
                    print(df[cols].head(10).to_string(index=False))
                    print("=" * 130)
                    print(f"{C.GREEN}✅ Monitor Active. Pairs: {len(results)}{C.END}\n")
                    first_run = False
                else:
                    ts = datetime.now().strftime('%H:%M:%S')
                    print(f"[{ts}] {C.GREEN}✅ Lighter Loop Finished ({len(results)} pairs).{C.END}")

            # Немає додаткового сну, бо цикл і так довгий

        except KeyboardInterrupt:
            print(f"\n{C.RED}🛑 Stopped{C.END}")
            break
        except Exception as e:
            print(f"\n{C.RED}❌ Error: {e}{C.END}")
            time.sleep(5)


if __name__ == "__main__":
    main()