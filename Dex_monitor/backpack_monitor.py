import requests
import pandas as pd
import concurrent.futures
import time
import sqlite3
import os
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ ШЛЯХІВ ТА ТАЙМЕРІВ
# ═══════════════════════════════════════════════════════════════════════════

API_BASE = 'https://api.backpack.exchange/api/v1'

# --- НАЛАШТУВАННЯ ПАПОК (Динамічні шляхи) ---
# Отримуємо шлях до папки, де лежить скрипт (Dex_monitor)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Виходимо на рівень вище (Root)
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Вказуємо шлях до папки Database (Root/Database)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'backpack_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

# --- ТАЙМЕРИ ---
UPDATE_INTERVAL_FAST = 15  # Ціна, Фандінг
UPDATE_INTERVAL_SLOW = 3600  # OI, Volume

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json'
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
# 🗄️ РОБОТА З БАЗОЮ ДАНИХ
# ═══════════════════════════════════════════════════════════════════════════

def init_db():
    """Створює зовнішню папку Database та таблицю"""
    if not os.path.exists(DB_FOLDER):
        try:
            os.makedirs(DB_FOLDER)
            print(f"{C.GREEN}📂 Створено папку: {DB_FOLDER}{C.END}")
        except OSError as e:
            print(f"{C.RED}❌ Помилка створення папки: {e}{C.END}")
            return

    conn = sqlite3.connect(DB_PATH)
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
    print(f"{C.GREEN}✅ База даних підключена: {DB_PATH}{C.END}")


def save_to_db(data_list, is_full_update):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        if is_full_update:
            # Повне оновлення (включаючи OI та Volume)
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
            # Швидке оновлення (Тільки ціни/фандінг)
            for row in data_list:
                cursor.execute('''
                    UPDATE market_data 
                    SET bid=?, ask=?, spread_pct=?, funding_pct=?, freq_hours=?, last_updated=?
                    WHERE token=?
                ''', (
                    row['Bid'], row['Ask'], row['Spread %'],
                    row['Funding %'], row['Freq (h)'], timestamp, row['Token']
                ))

                # Якщо токена немає - додаємо (UPSERT для нових лістингів)
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
    for i in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=10)
            if response.status_code == 429:
                time.sleep(1 + i)
                continue
            response.raise_for_status()
            return response.json()
        except Exception:
            if i == retries - 1: return None
            time.sleep(0.5)
    return None


def fetch_pair_data(symbol, ticker_data, full_update=False):
    # 1. СТАКАН
    depth = get_json(f"{API_BASE}/depth", params={'symbol': symbol, 'limit': 5})
    bid, ask = 0.0, 0.0
    if depth:
        try:
            bids = depth.get('bids', [])
            asks = depth.get('asks', [])
            if bids: bid = float(bids[0][0])
            if asks: ask = float(asks[0][0])
        except:
            pass

        # 2. FUNDING
    funding_res = get_json(f"{API_BASE}/fundingRates", params={'symbol': symbol, 'limit': 2})
    funding_pct = 0.0
    freq_hours = 1

    if funding_res and len(funding_res) > 0:
        funding_pct = float(funding_res[0].get('fundingRate', 0)) * 100
        if len(funding_res) >= 2:
            try:
                t1 = datetime.fromisoformat(funding_res[0]['intervalEndTimestamp'])
                t2 = datetime.fromisoformat(funding_res[1]['intervalEndTimestamp'])
                freq_hours = int((t1 - t2).total_seconds() / 3600)
            except:
                pass

    # 3. OI & VOLUME (Тільки якщо full_update)
    oi_usd = 0.0
    volume_usd = 0.0

    if full_update:
        oi_res = get_json(f"{API_BASE}/openInterest", params={'symbol': symbol})
        oi_tokens = 0.0
        if oi_res:
            if isinstance(oi_res, list) and len(oi_res) > 0:
                oi_tokens = float(oi_res[0].get('openInterest', 0))
            elif isinstance(oi_res, dict):
                oi_tokens = float(oi_res.get('openInterest', 0))

        last_price = float(ticker_data.get('lastPrice', 0))
        oi_usd = oi_tokens * last_price
        volume_usd = float(ticker_data.get('quoteVolume', 0))

    spread = 0.0
    if bid > 0:
        spread = ((ask - bid) / bid) * 100

    return {
        'Token': symbol.replace('_USDC_PERP', ''),
        'Bid': bid,
        'Ask': ask,
        'Spread %': spread,
        'Funding %': funding_pct,
        'Freq (h)': freq_hours,
        'OI ($)': oi_usd,
        'Volume 24h ($)': volume_usd
    }


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 BACKPACK MONITOR STARTED{C.END}")
    print(f"{C.YELLOW}📂 DB Path: {DB_PATH}{C.END}")

    init_db()

    last_slow_update = 0
    first_run = True  # Прапор для першого запуску

    while True:
        try:
            current_time = time.time()
            is_full_update = (current_time - last_slow_update) >= UPDATE_INTERVAL_SLOW

            # Лог про початок сканування (тільки якщо перший раз, інакше мовчимо)
            if first_run:
                print(f"{C.BOLD}🔄 Initial Scan (Full Data)...{C.END}")

            markets = get_json(f"{API_BASE}/markets")
            tickers = get_json(f"{API_BASE}/tickers")

            if not markets or not tickers:
                print(f"{C.RED}⚠️ API Error. Retrying in 5s...{C.END}")
                time.sleep(5)
                continue

            ticker_map = {t['symbol']: t for t in tickers}
            perp_symbols = [m['symbol'] for m in markets if m.get('marketType') == 'PERP']

            results = []

            # Сканування
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_symbol = {
                    executor.submit(fetch_pair_data, sym, ticker_map.get(sym, {}), is_full_update): sym
                    for sym in perp_symbols
                }

                completed = 0
                for future in concurrent.futures.as_completed(future_to_symbol):
                    data = future.result()
                    results.append(data)
                    completed += 1
                    # Показувати прогрес бар ТІЛЬКИ при першому запуску
                    if first_run:
                        print(f"\r⏳ Scanning: {completed}/{len(perp_symbols)}", end="", flush=True)

            # Збереження
            save_to_db(results, is_full_update)

            if is_full_update:
                last_slow_update = time.time()

            # --- ЛОГІКА ВИВОДУ ---
            ts = datetime.now().strftime('%H:%M:%S')

            # Розрахунок часу до наступного ПОВНОГО оновлення
            time_until_slow = max(0, UPDATE_INTERVAL_SLOW - (time.time() - last_slow_update))

            if first_run:
                # ПЕРШИЙ ЗАПУСК: Виводимо таблицю
                print("\n")
                df = pd.DataFrame(results)
                df = df.sort_values(by='Volume 24h ($)', ascending=False)
                cols = ['Token', 'Bid', 'Ask', 'Spread %', 'Funding %', 'Freq (h)', 'OI ($)', 'Volume 24h ($)']

                print("=" * 130)
                print(f"{C.BOLD}📊 INITIAL DATA (Top 10){C.END}")
                print(df[cols].head(10).to_string(index=False))
                print("=" * 130)
                print(f"{C.GREEN}✅ First run complete. Switching to monitoring mode.{C.END}\n")
                first_run = False
            else:
                # НАСТУПНІ ЗАПУСКИ: Тільки один рядок
                print(
                    f"[{ts}] {C.GREEN}✅ Backpack Updated.{C.END} Next Price/Fund: {UPDATE_INTERVAL_FAST}s | Next OI/Vol: {int(time_until_slow)}s")

            time.sleep(UPDATE_INTERVAL_FAST)

        except KeyboardInterrupt:
            print(f"\n{C.RED}🛑 Stopped by user{C.END}")
            break
        except Exception as e:
            print(f"\n{C.RED}❌ Critical Error: {e}{C.END}")
            time.sleep(5)


if __name__ == "__main__":
    main()