import requests
import pandas as pd
import time
import sqlite3
import os
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

API_URL = 'https://omni-client-api.prod.ap-northeast-1.variational.io/metadata/stats'

# Шляхи
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'variational_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

UPDATE_INTERVAL_FAST = 15
UPDATE_INTERVAL_SLOW = 3600

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}


class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


pd.set_option('display.max_rows', None)
pd.set_option('display.width', 250)
pd.set_option('display.float_format', '{:,.4f}'.format)


# ═══════════════════════════════════════════════════════════════════════════
# 🕒 ФУНКЦІЯ СИНХРОНІЗАЦІЇ (НОВЕ)
# ═══════════════════════════════════════════════════════════════════════════

def wait_for_next_cycle(interval=15):
    """
    Чекає до наступної "рівної" секунди (00, 15, 30, 45).
    """
    now = time.time()
    next_ts = (int(now) // interval + 1) * interval
    sleep_time = next_ts - now

    if sleep_time > 0:
        time.sleep(sleep_time)


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


def save_to_db(data_list, is_full_update):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        if is_full_update:
            for row in data_list:
                cursor.execute('''
                    INSERT OR REPLACE INTO market_data 
                    (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['Token'], row['Bid'], row['Ask'], row['Spread %'],
                    row['Funding %'], row['Freq (h)'], row['OI ($)'],
                    row['Volume 24h ($)'], timestamp
                ))
        else:
            for row in data_list:
                cursor.execute('''
                    UPDATE market_data 
                    SET bid=?, ask=?, spread_pct=?, funding_pct=?, freq_hours=?, last_updated=?
                    WHERE token=?
                ''', (
                    row['Bid'], row['Ask'], row['Spread %'],
                    row['Funding %'], row['Freq (h)'], timestamp, row['Token']
                ))
                if cursor.rowcount == 0:
                    cursor.execute('''
                        INSERT INTO market_data 
                        (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?)
                    ''', (
                        row['Token'], row['Bid'], row['Ask'], row['Spread %'],
                        row['Funding %'], row['Freq (h)'], timestamp
                    ))
        conn.commit()
    except Exception as e:
        print(f"{C.RED}❌ DB Error: {e}{C.END}")
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# 📡 API ФУНКЦІЇ
# ═══════════════════════════════════════════════════════════════════════════

def get_json(url, retries=3):
    for i in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=20)
            if response.status_code != 200:
                print(f"{C.RED}⚠️ API Status: {response.status_code}{C.END}")

            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"{C.RED}❌ Req Error: {e}{C.END}")
            if i == retries - 1: return None
            time.sleep(2)
    return None


def fetch_variational_data():
    raw_data = get_json(API_URL)

    if not raw_data:
        return []

    listings = raw_data.get('listings', [])

    if not listings:
        print(f"{C.YELLOW}⚠️ No 'listings' found.{C.END}")
        return []

    results = []

    for item in listings:
        try:
            ticker = item.get('ticker')
            if not ticker: continue

            # 1. Ціни
            quotes = item.get('quotes') or {}
            size_1k = quotes.get('size_1k') or {}

            bid = float(size_1k.get('bid', 0))
            ask = float(size_1k.get('ask', 0))

            # 2. Spread
            spread = 0.0
            if bid > 0:
                spread = ((ask - bid) / bid) * 100

            # 3. Funding Rate (BPS -> %)
            funding_bps = float(item.get('funding_rate', 0))
            funding_pct = funding_bps / 100.0

            # 4. Frequency
            interval_s = int(item.get('funding_interval_s', 3600))
            freq_hours = int(interval_s / 3600)

            # 5. Open Interest (USD)
            oi_data = item.get('open_interest') or {}
            long_oi = float(oi_data.get('long_open_interest', 0))
            short_oi = float(oi_data.get('short_open_interest', 0))
            oi_usd = long_oi + short_oi

            # 6. Volume
            vol_usd = float(item.get('volume_24h', 0))

            results.append({
                'Token': ticker,
                'Bid': bid,
                'Ask': ask,
                'Spread %': spread,
                'Funding %': funding_pct,
                'Freq (h)': freq_hours,
                'OI ($)': oi_usd,
                'Volume 24h ($)': vol_usd
            })

        except Exception as e:
            continue

    return results


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 VARIATIONAL MONITOR (SYNCED){C.END}")
    print(f"{C.YELLOW}📂 DB Path: {DB_PATH}{C.END}")

    init_db()

    last_slow_update = 0
    first_run = True

    while True:
        # 🔥 1. СИНХРОНІЗАЦІЯ: Чекаємо старту циклу
        wait_for_next_cycle(UPDATE_INTERVAL_FAST)

        try:
            current_time = time.time()
            is_full_update = (current_time - last_slow_update) >= UPDATE_INTERVAL_SLOW

            if first_run:
                print(f"{C.BOLD}🔄 Fetching Data...{C.END}")

            # 🔥 2. ОТРИМАННЯ ДАНИХ (Синхронно)
            data_list = fetch_variational_data()

            if not data_list:
                print(f"{C.RED}⚠️ No data. Retrying next cycle...{C.END}")
                continue

            # 🔥 3. ЗБЕРЕЖЕННЯ
            save_to_db(data_list, is_full_update)

            if is_full_update:
                last_slow_update = time.time()

            ts = datetime.now().strftime('%H:%M:%S')

            if first_run:
                print(f"{C.GREEN}✅ Monitor Active. Pairs: {len(data_list)}{C.END}\n")
                first_run = False
            else:
                print(f"{C.CYAN}[{ts}] Variational: оновив {len(data_list)} токенів.{C.END}")

            # time.sleep більше не потрібен

        except KeyboardInterrupt:
            print(f"\n{C.RED}🛑 Stopped{C.END}")
            break
        except Exception as e:
            print(f"\n{C.RED}❌ Error: {e}{C.END}")
            time.sleep(5)


if __name__ == "__main__":
    main()