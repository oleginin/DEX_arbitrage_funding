import requests
import pandas as pd
import time
import sqlite3
import os
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

API_URL = "https://api.starknet.extended.exchange/api/v1/info/markets"

# --- ШЛЯХИ ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'extended_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# --- ТАЙМЕРИ ---
UPDATE_INTERVAL_FAST = 15
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


def save_to_db(data_list, is_full_update):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # Єдина логіка для обох типів оновлення:
        # 1. Пробуємо ОНОВИТИ існуючий запис (UPDATE)
        # 2. Якщо запису немає -> СТВОРЮЄМО новий (INSERT)

        for row in data_list:
            if is_full_update:
                # Повне оновлення (всі поля)
                cursor.execute('''
                    UPDATE market_data 
                    SET bid=?, ask=?, spread_pct=?, funding_pct=?, freq_hours=?, oi_usd=?, volume_24h=?, last_updated=?
                    WHERE token=?
                ''', (
                    row['Bid'], row['Ask'], row['Spread %'],
                    row['Funding %'], row['Freq (h)'], row['OI ($)'],
                    row['Volume 24h ($)'], timestamp, row['Token']
                ))

                # Якщо UPDATE нічого не змінив (рядків 0), значить токена немає - робимо INSERT
                if cursor.rowcount == 0:
                    cursor.execute('''
                        INSERT INTO market_data 
                        (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['Token'], row['Bid'], row['Ask'], row['Spread %'],
                        row['Funding %'], row['Freq (h)'], row['OI ($)'],
                        row['Volume 24h ($)'], timestamp
                    ))

            else:
                # Швидке оновлення (Тільки ціна і фандінг, OI/Vol не чіпаємо)
                cursor.execute('''
                    UPDATE market_data 
                    SET bid=?, ask=?, spread_pct=?, funding_pct=?, freq_hours=?, last_updated=?
                    WHERE token=?
                ''', (
                    row['Bid'], row['Ask'], row['Spread %'],
                    row['Funding %'], row['Freq (h)'], timestamp, row['Token']
                ))

                # Якщо нового токена ще немає, вставляємо його з нульовими OI/Vol
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
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                print(f"{C.RED}⚠️ API Status: {response.status_code}{C.END}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"{C.RED}❌ Req Error: {e}{C.END}")
            if i == retries - 1: return None
            time.sleep(2)
    return None


def fetch_extended_data():
    # Отримуємо ВСІ маркети одним запитом
    raw_response = get_json(API_URL)

    if not raw_response:
        return []

    # Перевірка успішності запиту
    if raw_response.get('status') != 'OK':
        print(f"{C.YELLOW}⚠️ API returned status: {raw_response.get('status')}{C.END}")
        return []

    # Список токенів лежить у 'data'
    markets = raw_response.get('data', [])

    results = []

    for m in markets:
        try:
            # 1. Фільтр: Тільки активні
            if m.get('status') != 'ACTIVE':
                continue

            stats = m.get('marketStats', {})
            raw_ticker = m.get('name')  # Приходить наприклад "ENA-USD"

            # 🔥 ВИПРАВЛЕННЯ НАЗВИ 🔥
            # Видаляємо "-USD" з кінця, якщо воно там є
            ticker = raw_ticker.replace('-USD', '')

            # 2. Ціни
            bid = float(stats.get('bidPrice', 0))
            ask = float(stats.get('askPrice', 0))

            # 3. Spread
            spread = 0.0
            if bid > 0:
                spread = ((ask - bid) / bid) * 100

            # 4. Фандінг (множимо на 100, бо це 1-годинна ставка)
            funding_raw = float(stats.get('fundingRate', 0))
            funding_pct = funding_raw * 100.0

            # 5. OI & Volume (Вже в USD)
            oi_usd = float(stats.get('openInterest', 0))
            vol_usd = float(stats.get('dailyVolume', 0))

            results.append({
                'Token': ticker,
                'Bid': bid,
                'Ask': ask,
                'Spread %': spread,
                'Funding %': funding_pct,
                'Freq (h)': 1,  # Extended має 1-годинний фандінг
                'OI ($)': oi_usd,
                'Volume 24h ($)': vol_usd
            })

        except Exception:
            continue

    return results


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 EXTENDED EXCHANGE MONITOR{C.END}")
    print(f"{C.YELLOW}📂 DB Path: {DB_PATH}{C.END}")

    init_db()

    last_slow_update = 0
    first_run = True

    while True:
        try:
            current_time = time.time()
            is_full_update = (current_time - last_slow_update) >= UPDATE_INTERVAL_SLOW

            if first_run:
                print(f"{C.BOLD}🔄 Fetching Data...{C.END}")

            data_list = fetch_extended_data()

            if not data_list:
                print(f"{C.RED}⚠️ No data fetched. Retrying in 5s...{C.END}")
                time.sleep(5)
                continue

            save_to_db(data_list, is_full_update)

            if is_full_update:
                last_slow_update = time.time()

            ts = datetime.now().strftime('%H:%M:%S')

            if first_run:
                # ПЕРШИЙ ЗАПУСК
                print(f"{C.GREEN}✅ Monitor Active. Pairs found: {len(data_list)}{C.END}\n")
                first_run = False
            else:
                # НАСТУПНІ ЗАПУСКИ
                print(f"{C.CYAN}[{ts}] Extended: оновив {len(data_list)} токенів.{C.END}")

            time.sleep(UPDATE_INTERVAL_FAST)

        except KeyboardInterrupt:
            print(f"\n{C.RED}🛑 Stopped{C.END}")
            break
        except Exception as e:
            print(f"\n{C.RED}❌ Error: {e}{C.END}")
            time.sleep(5)


if __name__ == "__main__":
    main()