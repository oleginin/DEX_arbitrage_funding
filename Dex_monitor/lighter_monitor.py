import websocket
import requests
import json
import sqlite3
import time
import threading
import os
import sys
from datetime import datetime  # <--- ВАЖЛИВО!

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
REST_API_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails?filter=perp"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')
DB_NAME = 'lighter_database.db'
DB_PATH = os.path.join(DB_FOLDER, DB_NAME)

id_to_symbol = {}
local_books = {}
market_stats_cache = {}
data_lock = threading.Lock()


class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════
# 🗄️ БАЗА ДАНИХ (ОПТИМІЗОВАНО)
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
    # Даємо трохи часу на старті
    time.sleep(5)

    while True:
        try:
            data_to_save = []

            with data_lock:
                for mid, symbol in id_to_symbol.items():
                    # 1. Ціни
                    book = local_books.get(mid)
                    if not book: continue

                    best_bid = max(book['bids'].keys()) if book['bids'] else 0.0
                    best_ask = min(book['asks'].keys()) if book['asks'] else 0.0

                    if best_bid == 0: continue

                    spread = 0.0
                    if best_ask > 0:
                        spread = ((best_ask - best_bid) / best_bid) * 100

                    # 2. Статистика
                    stats = market_stats_cache.get(mid, {})

                    funding = stats.get('funding', 0.0)
                    vol_usd = stats.get('vol', 0.0)
                    oi_usd = stats.get('oi', 0.0) * 2.0

                    data_to_save.append({
                        'token': symbol,
                        'bid': best_bid,
                        'ask': best_ask,
                        'spread': spread,
                        'funding': funding,
                        'oi': oi_usd,
                        'vol': vol_usd
                    })

            if data_to_save:
                conn = sqlite3.connect(DB_PATH, timeout=10)  # Додав таймаут
                cursor = conn.cursor()

                # 🔥 ВИПРАВЛЕНО: Повний формат дати, як у інших моніторах
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                for row in data_to_save:
                    cursor.execute('''
                        INSERT OR REPLACE INTO market_data 
                        (token, bid, ask, spread_pct, funding_pct, freq_hours, oi_usd, volume_24h, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        row['token'], row['bid'], row['ask'], row['spread'],
                        row['funding'], 1, row['oi'], row['vol'], ts
                    ))

                conn.commit()

                # 🔥 ВАЖЛИВО: Примусово оновлюємо WAL файл, щоб агрегатор побачив зміни
                conn.execute('PRAGMA wal_checkpoint(PASSIVE);')

                conn.close()

                print(f"{C.CYAN}[{ts.split()[1]}] Lighter: оновив {len(data_to_save)} токенів.{C.END}")

            time.sleep(15)

        except Exception as e:
            print(f"\n{C.RED}❌ DB Loop Error: {e}{C.END}")
            time.sleep(5)


# ═══════════════════════════════════════════════════════════════════════════
# 🌐 WEBSOCKET LOGIC
# ═══════════════════════════════════════════════════════════════════════════

def get_market_map():
    try:
        r = requests.get(REST_API_URL, headers={'accept': 'application/json'}, timeout=10)
        data = r.json()
        mapping = {}
        for item in data.get('order_book_details', []):
            if item.get('status') == 'active':
                if float(item.get('daily_quote_token_volume', 0)) > 10:
                    mapping[item['market_id']] = item['symbol']
        return mapping
    except Exception as e:
        print(f"{C.RED}❌ Init Error: {e}{C.END}")
        return {}


def on_message(ws, message):
    try:
        data = json.loads(message)
        msg_type = data.get('type')

        with data_lock:
            if msg_type == 'update/market_stats':
                all_stats_map = data.get('market_stats', {})
                for mid_str, stats in all_stats_map.items():
                    try:
                        mid = int(mid_str)
                    except:
                        continue
                    raw_funding = float(stats.get('current_funding_rate', 0) or 0)
                    vol_usd = float(stats.get('daily_quote_token_volume', 0) or 0)
                    oi_usd_raw = float(stats.get('open_interest', 0) or 0)

                    market_stats_cache[mid] = {
                        'funding': raw_funding,
                        'vol': vol_usd,
                        'oi': oi_usd_raw
                    }

            elif msg_type == 'update/order_book':
                channel = data.get('channel', '')
                try:
                    mid = int(channel.split(':')[1])
                except:
                    return

                if mid not in id_to_symbol: return

                if mid not in local_books:
                    local_books[mid] = {'bids': {}, 'asks': {}}

                ob_data = data.get('order_book', {})

                for b in ob_data.get('bids', []):
                    price = float(b['price'])
                    size = float(b['size'])
                    if size == 0:
                        local_books[mid]['bids'].pop(price, None)
                    else:
                        local_books[mid]['bids'][price] = size

                for a in ob_data.get('asks', []):
                    price = float(a['price'])
                    size = float(a['size'])
                    if size == 0:
                        local_books[mid]['asks'].pop(price, None)
                    else:
                        local_books[mid]['asks'][price] = size

    except Exception as e:
        pass


def on_error(ws, error):
    print(f"\n{C.RED}⚠️ WSS Error: {error}{C.END}")


def on_close(ws, close_status_code, close_msg):
    print(f"\n{C.YELLOW}🔌 WSS Closed. Reconnecting...{C.END}")


def on_open(ws):
    print(f"{C.GREEN}✅ WSS Connected! Subscribing...{C.END}")

    ws.send(json.dumps({
        "type": "subscribe",
        "channel": "market_stats/all"
    }))

    def subscribe_books():
        time.sleep(1)
        count = 0
        for mid in id_to_symbol.keys():
            msg = json.dumps({
                "type": "subscribe",
                "channel": f"order_book/{mid}"
            })
            ws.send(msg)
            count += 1
            if count % 20 == 0: time.sleep(0.1)

        print(f"{C.GREEN}✅ Subscribed to {count} order books.{C.END}")

    threading.Thread(target=subscribe_books).start()


def main():
    global id_to_symbol
    print(f"\n{C.CYAN}🚀 LIGHTER WSS MONITOR (15s update){C.END}")

    init_db()

    print(f"{C.BOLD}🔄 Fetching market map (REST)...{C.END}")
    id_to_symbol = get_market_map()
    print(f"{C.GREEN}✅ Loaded {len(id_to_symbol)} pairs.{C.END}")

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
            ws.run_forever()
        except Exception as e:
            print(f"❌ Critical WSS Error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()