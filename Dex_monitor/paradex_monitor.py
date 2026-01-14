import requests
import pandas as pd
import time
import sqlite3
import os
import concurrent.futures
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

API_BASE = "https://api.prod.paradex.trade/v1"

# --- ШЛЯХИ ДО БАЗИ ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'paradex_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# --- ТАЙМЕРИ ---
UPDATE_INTERVAL_FAST = 15  # Інтервал синхронізації
UPDATE_INTERVAL_SLOW = 3600

HEADERS = {
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
        except OSError:
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

def get_json(url, params=None, retries=3):
    """Виконує GET запит з обробкою помилок"""
    for i in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=10)

            # Якщо Paradex повертає 429 (Rate Limit), чекаємо
            if response.status_code == 429:
                time.sleep(1 + i)
                continue

            response.raise_for_status()
            return response.json()
        except Exception as e:
            if i == retries - 1:
                return None
            time.sleep(0.5)
    return None


def get_markets_meta():
    """Завантажує список всіх PERP пар"""
    data = get_json(f"{API_BASE}/markets")
    meta_map = {}
    if data:
        results = data.get('results', [])
        for m in results:
            symbol = m.get('symbol', '')
            if m.get('asset_kind') == 'PERP':
                # За замовчуванням 1 година
                freq = m.get('funding_period_hours', 1)
                # Зберігаємо frequency для символу
                meta_map[symbol] = freq
    return meta_map


def fetch_pair_summary(symbol, freq):
    """
    Отримує дані для ОДНІЄЇ пари.
    """
    data = get_json(f"{API_BASE}/markets/summary", params={'market': symbol})

    if not data or 'results' not in data or not data['results']:
        return None

    try:
        item = data['results'][0]

        bid = float(item.get('bid', 0))
        ask = float(item.get('ask', 0))
        mark_price = float(item.get('mark_price', 0))
        vol_24h = float(item.get('volume_24h', 0))

        oi_tokens = float(item.get('open_interest', 0))
        oi_usd = oi_tokens * mark_price

        funding_raw = float(item.get('funding_rate', 0))
        funding_pct = funding_raw * 100

        spread = 0.0
        if bid > 0:
            spread = ((ask - bid) / bid) * 100

        return {
            'Token': symbol.replace('-USD-PERP', ''),
            'Bid': bid,
            'Ask': ask,
            'Spread %': spread,
            'Funding %': funding_pct,
            'Freq (h)': freq,
            'OI ($)': oi_usd,
            'Volume 24h ($)': vol_24h
        }
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 PARADEX MONITOR STARTED (SYNCED){C.END}")

    init_db()

    print(f"{C.BOLD}🔄 Loading Metadata...{C.END}")
    freq_map = get_markets_meta()

    if not freq_map:
        print(f"{C.RED}❌ Failed to fetch markets. Check connection.{C.END}")
        return

    print(f"{C.GREEN}✅ Loaded {len(freq_map)} PERP pairs.{C.END}")

    last_slow_update = 0
    first_run = True

    # Список символів для сканування
    symbols = list(freq_map.keys())

    while True:
        # 🔥 1. СИНХРОНІЗАЦІЯ: Чекаємо старту циклу
        wait_for_next_cycle(UPDATE_INTERVAL_FAST)

        try:
            current_time = time.time()
            is_full_update = (current_time - last_slow_update) >= UPDATE_INTERVAL_SLOW

            if first_run:
                print(f"{C.BOLD}🔄 Fetching live data (Threads)...{C.END}")

            results = []

            # 🔥 2. ОТРИМАННЯ ДАНИХ (Починається синхронно)
            # Багатопотоковий запуск (20 потоків)
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_symbol = {
                    executor.submit(fetch_pair_summary, sym, freq_map[sym]): sym
                    for sym in symbols
                }

                completed = 0
                for future in concurrent.futures.as_completed(future_to_symbol):
                    data = future.result()
                    if data:
                        results.append(data)

                    completed += 1
                    # Прогрес бар тільки для першого запуску
                    if first_run:
                        print(f"\r⏳ Progress: {completed}/{len(symbols)}", end="", flush=True)

            if not results:
                print(f"\n{C.RED}⚠️ No data fetched. API might be blocking or down.{C.END}")
                # Якщо API лежить, чекаємо наступного циклу, не спимо вручну
                continue

            # 🔥 3. ЗБЕРЕЖЕННЯ
            save_to_db(results, is_full_update)

            if is_full_update:
                last_slow_update = time.time()

            # Вивід
            ts = datetime.now().strftime('%H:%M:%S')

            if first_run:
                print(f"{C.GREEN}✅ Monitor Active.{C.END}\n")
                first_run = False
            else:
                print(f"{C.CYAN}[{ts}] Paradex: оновив {len(results)} токенів.{C.END}")

            # time.sleep більше не потрібен

        except KeyboardInterrupt:
            print(f"\n{C.RED}🛑 Stopped{C.END}")
            break
        except Exception as e:
            print(f"\n{C.RED}❌ Error: {e}{C.END}")
            time.sleep(5)


if __name__ == "__main__":
    main()