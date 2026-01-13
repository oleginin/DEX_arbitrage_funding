import sqlite3
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from contextlib import closing  # 🔥 Ця штука гарантує закриття

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

PAUSE_AFTER_UPDATE = 15
RESET_HISTORY_ON_START = True  # Очищати історію при старті
STATS_WARMUP_SEC = 60  # Час розігріву (без історії)

# 🛑 ФІЛЬТРИ
MIN_OI_USD = 500000
MIN_VOL_USD = 500000
MAX_DATA_DELAY_SEC = 60

# --- ШЛЯХИ ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')

SOURCE_DBS = [
    {'name': 'Backpack', 'file': 'backpack_database.db'},
    {'name': 'Paradex', 'file': 'paradex_database.db'},
    {'name': 'Variational', 'file': 'variational_database.db'},
    {'name': 'Extended', 'file': 'extended_database.db'},
    # {'name': 'Lighter', 'file': 'lighter_database.db'},
]

TARGET_DB_NAME = 'arbitrage_dashboard.db'
TARGET_DB_PATH = os.path.join(DB_FOLDER, TARGET_DB_NAME)

SCRIPT_START_TIME = time.time()


class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


# ═══════════════════════════════════════════════════════════════════════════
# 🛠️ ІНІЦІАЛІЗАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

def init_target_db():
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)

    # Використовуємо closing(), щоб з'єднання точно закрилося
    with closing(sqlite3.connect(TARGET_DB_PATH)) as conn:
        conn.execute('PRAGMA journal_mode=WAL;')
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS live_opportunities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT,
                route TEXT,
                buy_exchange TEXT,
                sell_exchange TEXT,
                buy_price REAL,
                sell_price REAL,
                spread_pct REAL,
                spread_min_24h REAL,
                spread_max_24h REAL,
                spread_min_30d REAL,
                spread_max_30d REAL,
                net_funding_pct REAL,
                funding_freq TEXT,
                oi_long_usd REAL,
                oi_short_usd REAL,
                vol_long_usd REAL,
                vol_short_usd REAL,
                last_updated TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS spread_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT,
                route TEXT, 
                spread_pct REAL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hist_route_time ON spread_history (route, timestamp);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hist_time ON spread_history (timestamp);')

        conn.commit()

        if RESET_HISTORY_ON_START:
            try:
                cursor.execute("DELETE FROM spread_history")
                conn.commit()
                # 🔥 TRUNCATE примусово очищає WAL файл
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                print(f"{C.RED}🧹 History CLEARED & WAL Truncated.{C.END}")
            except Exception as e:
                print(f"⚠️ Clean error: {e}")

    print(f"{C.GREEN}✅ Target DB Initialized.{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 📥 ЧИТАННЯ (SAFE MODE)
# ═══════════════════════════════════════════════════════════════════════════

def get_data_from_source(db_config):
    db_path = os.path.join(DB_FOLDER, db_config['file'])
    if not os.path.exists(db_path): return None

    try:
        # closing() гарантує закриття навіть при помилках
        with closing(sqlite3.connect(db_path, timeout=10, isolation_level=None)) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            # PASSIVE просто читає, не блокуючи інших
            conn.execute('PRAGMA wal_checkpoint(PASSIVE);')

            query = "SELECT * FROM market_data"
            df = pd.read_sql_query(query, conn)

            if df.empty: return None

            df['last_updated'] = pd.to_datetime(df['last_updated'])
            cutoff_time = datetime.now() - timedelta(seconds=MAX_DATA_DELAY_SEC)
            fresh_df = df[df['last_updated'] > cutoff_time].copy()

            if fresh_df.empty: return None

            fresh_df['exchange'] = db_config['name']
            if 'freq_hours' not in fresh_df.columns: fresh_df['freq_hours'] = 1

            return fresh_df

    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 🧠 РОЗРАХУНОК
# ═══════════════════════════════════════════════════════════════════════════

def get_effective_funding(row):
    exch = row['exchange']
    raw_fund = row['funding_pct']
    freq = max(1, row['freq_hours'])
    if exch == 'Variational':
        return (raw_fund * freq), freq
    else:
        return raw_fund, freq


def calculate_live_routes(all_data_df):
    if all_data_df.empty: return pd.DataFrame()
    results = []
    grouped = all_data_df.groupby('token')

    for token, group in grouped:
        if len(group) < 2: continue

        potential_buys = group[group['ask'] > 0]
        potential_sells = group[group['bid'] > 0]

        for _, buy_row in potential_buys.iterrows():
            if buy_row['oi_usd'] < MIN_OI_USD or buy_row['volume_24h'] < MIN_VOL_USD: continue

            for _, sell_row in potential_sells.iterrows():
                if sell_row['oi_usd'] < MIN_OI_USD or sell_row['volume_24h'] < MIN_VOL_USD: continue
                if buy_row['exchange'] == sell_row['exchange']: continue

                buy_price = buy_row['ask']
                sell_price = sell_row['bid']
                spread = ((sell_price - buy_price) / buy_price) * 100

                fund_long_pct, freq_long = get_effective_funding(buy_row)
                fund_short_pct, freq_short = get_effective_funding(sell_row)

                hourly_long = fund_long_pct / freq_long
                hourly_short = fund_short_pct / freq_short
                max_freq = max(freq_long, freq_short)
                net_funding_scaled = (hourly_short - hourly_long) * max_freq

                freq_str = f"{int(freq_long)}h / {int(freq_short)}h"
                route_name = f"{buy_row['exchange']} ➡️ {sell_row['exchange']}"

                results.append({
                    'token': token,
                    'route': route_name,
                    'buy_exchange': buy_row['exchange'],
                    'sell_exchange': sell_row['exchange'],
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'spread': spread,
                    'net_funding': net_funding_scaled,
                    'funding_freq_str': freq_str,
                    'oi_long': buy_row['oi_usd'],
                    'oi_short': sell_row['oi_usd'],
                    'vol_long': buy_row['volume_24h'],
                    'vol_short': sell_row['volume_24h']
                })

    return pd.DataFrame(results)


# ═══════════════════════════════════════════════════════════════════════════
# 💾 СТАТИСТИКА (SAFE MODE)
# ═══════════════════════════════════════════════════════════════════════════

def update_history_and_get_stats(df_live):
    if df_live.empty: return df_live

    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")

            history_data = []
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            for _, row in df_live.iterrows():
                history_data.append((row['token'], row['route'], row['spread'], now_str))

            cursor.executemany(
                "INSERT INTO spread_history (token, route, spread_pct, timestamp) VALUES (?, ?, ?, ?)",
                history_data
            )

            cursor.execute("DELETE FROM spread_history WHERE timestamp < datetime('now', '-30 days', '-1 hour')")
            conn.commit()

            elapsed_time = time.time() - SCRIPT_START_TIME

            if elapsed_time < STATS_WARMUP_SEC:
                df_live['min_24h'] = df_live['spread']
                df_live['max_24h'] = df_live['spread']
                df_live['min_30d'] = df_live['spread']
                df_live['max_30d'] = df_live['spread']
                return df_live
            else:
                stats_query = """
                SELECT 
                    route,
                    MIN(CASE WHEN timestamp >= datetime('now', '-1 day') THEN spread_pct END) as min_24h,
                    MAX(CASE WHEN timestamp >= datetime('now', '-1 day') THEN spread_pct END) as max_24h,
                    MIN(spread_pct) as min_30d,
                    MAX(spread_pct) as max_30d
                FROM spread_history
                WHERE timestamp >= datetime('now', '-30 days')
                GROUP BY route
                """
                df_stats = pd.read_sql_query(stats_query, conn)

                if not df_stats.empty:
                    df_final = pd.merge(df_live, df_stats, on='route', how='left')
                    for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                        df_final[col] = df_final[col].fillna(df_final['spread'])
                    return df_final
                else:
                    for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                        df_live[col] = df_live['spread']
                    return df_live

    except Exception as e:
        print(f"{C.RED}❌ Stats Error: {e}{C.END}")
        return df_live

    # ═══════════════════════════════════════════════════════════════════════════


# 🔄 ОНОВЛЕННЯ LIVE (З TRUNCATE)
# ═══════════════════════════════════════════════════════════════════════════

def update_dashboard_db(df_final):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")
            cursor.execute("DELETE FROM live_opportunities")

            data_to_insert = []
            if not df_final.empty:
                for _, row in df_final.iterrows():
                    data_to_insert.append((
                        row['token'], row['route'], row['buy_exchange'], row['sell_exchange'],
                        row['buy_price'], row['sell_price'], row['spread'],
                        row['min_24h'], row['max_24h'], row['min_30d'], row['max_30d'],
                        row['net_funding'], row['funding_freq_str'],
                        row['oi_long'], row['oi_short'], row['vol_long'], row['vol_short'],
                        timestamp
                    ))

                cursor.executemany('''
                    INSERT INTO live_opportunities 
                    (token, route, buy_exchange, sell_exchange, buy_price, sell_price, 
                    spread_pct, spread_min_24h, spread_max_24h, spread_min_30d, spread_max_30d,
                    net_funding_pct, funding_freq, 
                    oi_long_usd, oi_short_usd, vol_long_usd, vol_short_usd, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', data_to_insert)

            conn.commit()

            # 🔥 МАГІЯ: Примусово обрізаємо WAL файл, щоб він зник (або став 0 байт)
            cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    except Exception as e:
        print(f"{C.RED}❌ Write Error: {e}{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 ARBITRAGE AGGREGATOR (AUTO-CLOSE WAL){C.END}")
    print(f"{C.YELLOW}⏳ Warmup: {STATS_WARMUP_SEC}s | Reset History: {RESET_HISTORY_ON_START}{C.END}")

    init_target_db()

    while True:
        start_time = time.time()
        dfs = []
        total_rows_read = 0

        for db_conf in SOURCE_DBS:
            df = get_data_from_source(db_conf)
            if df is not None and not df.empty:
                dfs.append(df)
                total_rows_read += len(df)

        if not dfs:
            print(f"\r{C.RED}⚠️ Waiting for FRESH data...{C.END}", end="")
            time.sleep(1)
            continue

        full_market_data = pd.concat(dfs, ignore_index=True)
        df_live = calculate_live_routes(full_market_data)
        df_final = update_history_and_get_stats(df_live)

        if not df_final.empty:
            df_final = df_final.sort_values(by='spread', ascending=False)

        update_dashboard_db(df_final)

        duration = time.time() - start_time
        count = len(df_final)
        top_spread = df_final.iloc[0]['spread'] if not df_final.empty else 0.0

        ts = datetime.now().strftime('%H:%M:%S')

        elapsed = time.time() - SCRIPT_START_TIME
        warmup_status = ""
        if elapsed < STATS_WARMUP_SEC:
            warmup_status = f"{C.YELLOW}[WARMUP {int(elapsed)}s]{C.END} "

        print(f"\r{C.CYAN}[{ts}] {warmup_status}Routes: {count}. Top: {top_spread:.2f}%. Took: {duration:.3f}s{C.END}",
              end="")

        time.sleep(PAUSE_AFTER_UPDATE)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{C.RED}🛑 Stopped.{C.END}")