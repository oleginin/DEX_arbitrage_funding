import websocket
import requests
import json
import sqlite3
import time
import threading
import os
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

WS_URL = "wss://ws.backpack.exchange"
REST_API_URL = "https://api.backpack.exchange/api/v1"

# --- ШЛЯХИ ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'backpack_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

UPDATE_INTERVAL_FAST = 15

# --- ГЛОБАЛЬНЕ СХОВИЩЕ ---
local_books = {}  # Стакани
market_stats = {}  # Статистика
symbols_map = []
data_lock = threading.Lock()


class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════
# 🕒 СИНХРОНІЗАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

def wait_for_next_cycle(interval=15):
    now = time.time()
    next_ts = (int(now) // interval + 1) * interval
    sleep_time = next_ts - now
    if sleep_time > 0:
        time.sleep(sleep_time)


# ═══════════════════════════════════════════════════════════════════════════
# 🛠️ ХЕЛПЕР: НОРМАЛІЗАЦІЯ ІМЕНІ
# ═══════════════════════════════════════════════════════════════════════════

def get_clean_symbol(raw_symbol):
    return raw_symbol.replace('_USDC_PERP', '').replace('_USDC', '').replace('_PERP', '')


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


def update_db_loop():
    time.sleep(2)

    while True:
        wait_for_next_cycle(UPDATE_INTERVAL_FAST)

        try:
            data_to_save = []

            with data_lock:
                all_tokens = set(local_books.keys()) | set(market_stats.keys())

                for clean_token in all_tokens:
                    book = local_books.get(clean_token)
                    stats = market_stats.get(clean_token, {})

                    if not book or not book.get('bids') or not book.get('asks'):
                        continue

                    try:
                        best_bid = max(book['bids'].keys())
                        best_ask = min(book['asks'].keys())
                    except ValueError:
                        continue

                    if best_bid == 0 or best_ask == 0: continue

                    spread = ((best_ask - best_bid) / best_bid) * 100

                    price_calc = stats.get('mark_price', 0)
                    if price_calc == 0: price_calc = (best_bid + best_ask) / 2

                    oi_usd = stats.get('oi_contracts', 0) * 2 * price_calc

                    data_to_save.append({
                        'Token': clean_token,
                        'Bid': best_bid,
                        'Ask': best_ask,
                        'Spread %': spread,
                        'Funding %': stats.get('funding', 0.0),
                        'Freq (h)': 1,
                        'OI ($)': oi_usd,
                        'Volume 24h ($)': stats.get('vol', 0.0)
                    })

            if data_to_save:
                conn = sqlite3.connect(DB_PATH, timeout=5)
                cursor = conn.cursor()
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                for row in data_to_save:
                    cursor.execute('''
                        INSERT OR REPLACE INTO market_data 
                        (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['Token'], row['Bid'], row['Ask'], row['Spread %'],
                        row['Funding %'], row['Freq (h)'], row['OI ($)'],
                        row['Volume 24h ($)'], ts
                    ))

                conn.commit()
                conn.execute('PRAGMA wal_checkpoint(PASSIVE);')
                conn.close()

                print(f"{C.CYAN}[{ts.split()[1]}] Backpack (WSS): оновив {len(data_to_save)} токенів.{C.END}")

        except Exception as e:
            print(f"{C.RED}❌ DB Loop Error: {e}{C.END}")
            time.sleep(1)


# ═══════════════════════════════════════════════════════════════════════════
# 🌐 WEBSOCKET LOGIC
# ═══════════════════════════════════════════════════════════════════════════

def get_perp_symbols():
    try:
        r = requests.get(f"{REST_API_URL}/markets", timeout=10)
        data = r.json()
        perps = [m['symbol'] for m in data if m.get('marketType') == 'PERP']
        return perps
    except:
        return []


def on_message(ws, message):
    try:
        payload = json.loads(message)
        data = payload.get('data')
        if not data: return

        raw_symbol = data.get('s')
        event_type = data.get('e')

        if not raw_symbol or not event_type: return

        clean_symbol = get_clean_symbol(raw_symbol)

        with data_lock:
            if clean_symbol not in market_stats: market_stats[clean_symbol] = {}
            if clean_symbol not in local_books: local_books[clean_symbol] = {'bids': {}, 'asks': {}}

            if event_type == 'depth':
                for item in data.get('b', []):
                    price = float(item[0])
                    qty = float(item[1])
                    if qty == 0:
                        local_books[clean_symbol]['bids'].pop(price, None)
                    else:
                        local_books[clean_symbol]['bids'][price] = qty

                for item in data.get('a', []):
                    price = float(item[0])
                    qty = float(item[1])
                    if qty == 0:
                        local_books[clean_symbol]['asks'].pop(price, None)
                    else:
                        local_books[clean_symbol]['asks'][price] = qty

            elif event_type == 'ticker':
                market_stats[clean_symbol]['vol'] = float(data.get('V', 0))

            elif event_type == 'markPrice':
                market_stats[clean_symbol]['mark_price'] = float(data.get('p', 0))
                if 'f' in data:
                    market_stats[clean_symbol]['funding'] = float(data['f']) * 100

            elif event_type == 'openInterest':
                market_stats[clean_symbol]['oi_contracts'] = float(data.get('o', 0))

    except Exception as e:
        pass


def on_error(ws, error):
    if str(error):
        print(f"\n{C.RED}⚠️ WSS Error: {error}{C.END}")


def on_close(ws, close_status_code, close_msg):
    print(f"\n{C.YELLOW}🔌 WSS Closed. Reconnecting in 3s...{C.END}")
    time.sleep(3)
    with data_lock:
        local_books.clear()
        print(f"{C.YELLOW}🧹 Cleared orderbooks.{C.END}")


def on_open(ws):
    print(f"{C.GREEN}✅ WSS Connected! Subscribing...{C.END}")

    def subscribe_slowly():
        streams = []
        for sym in symbols_map:
            streams.append(f"depth.{sym}")
            streams.append(f"ticker.{sym}")
            streams.append(f"markPrice.{sym}")
            streams.append(f"openInterest.{sym}")

            # 🔥 ЗМЕНШЕНИЙ ЧАНК: По 10 стрімів (дуже обережно)
        chunk_size = 10
        total_chunks = len(streams) // chunk_size + 1

        for i in range(0, len(streams), chunk_size):
            chunk = streams[i:i + chunk_size]
            if not chunk: continue

            payload = {"method": "SUBSCRIBE", "params": chunk}
            try:
                ws.send(json.dumps(payload))
            except:
                break

            # 🔥 ЗБІЛЬШЕНА ПАУЗА: 1 секунда між пакетами
            # Це дає серверу час "переварити" підписку і не розірвати з'єднання
            time.sleep(1.0)

            print(f"\r⏳ Subscribing... {i}/{len(streams)} streams sent", end="", flush=True)

        print(f"\n{C.GREEN}✅ All subscriptions sent.{C.END}")

    threading.Thread(target=subscribe_slowly).start()


def main():
    global symbols_map
    print(f"\n{C.CYAN}🚀 BACKPACK WSS MONITOR (SLOW START MODE){C.END}")

    init_db()
    symbols_map = get_perp_symbols()

    if not symbols_map:
        print(f"{C.RED}❌ No PERP symbols found.{C.END}")
        return

    db_thread = threading.Thread(target=update_db_loop, daemon=True)
    db_thread.start()

    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # 🔥 ПОВЕРНУЛИ ПІНГ, АЛЕ М'ЯКИЙ
            # Інтервал 25с (щоб NAT не вбивав), Таймаут 20с (щоб не панікувати)
            ws.run_forever(ping_interval=25, ping_timeout=20)
        except Exception:
            time.sleep(5)


if __name__ == "__main__":
    main()