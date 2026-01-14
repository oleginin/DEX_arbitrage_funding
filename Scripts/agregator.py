import sqlite3
import pandas as pd
import time
import os
from datetime import datetime, timedelta
from contextlib import closing

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

PAUSE_AFTER_UPDATE = 15
RESET_HISTORY_ON_START = True
STATS_WARMUP_SEC = 60

# 🔥 НАЛАШТУВАННЯ ЧУТЛИВОСТІ
# Якщо спред менший за це число, він вважається "шумом" (пилом/помилкою)
# і ми НЕ дозволяємо йому ставати новим мінімумом.
MIN_HISTORY_SPREAD_THRESHOLD = 0.2

IGNORE_NEGATIVE_HISTORY = True

# 🛑 ФІЛЬТРИ (Вимкнені, щоб бачити все)
MIN_OI_USD = 0
MIN_VOL_USD = 0

# ⏰ СИНХРОНІЗАЦІЯ
MAX_DATA_DELAY_SEC = 60
MAX_SYNC_DIFF_SEC = 25  # Якщо хочеш жорсткіше - став 2-5 сек

# --- ШЛЯХИ ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DB_FOLDER = os.path.join(PROJECT_ROOT, 'Database')

SOURCE_DBS = [
    {'name': 'Backpack', 'file': 'backpack_database.db'},
    {'name': 'Paradex', 'file': 'paradex_database.db'},
    {'name': 'Variational', 'file': 'variational_database.db'},
    {'name': 'Extended', 'file': 'extended_database.db'},
    {'name': 'Lighter', 'file': 'lighter_database.db'},
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
# 🛠️ ІНІЦІАЛІЗАЦІЯ (ПРАВИЛЬНИЙ UNIQUE KEY)
# ═══════════════════════════════════════════════════════════════════════════

def init_target_db():
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)

    with closing(sqlite3.connect(TARGET_DB_PATH)) as conn:
        conn.execute('PRAGMA journal_mode=WAL;')
        cursor = conn.cursor()

        # 🔥 ЗМІНА: Ключ унікальності тепер (token, buy_exchange, sell_exchange)
        # Це гарантує, що "Lighter -> Backpack" і "Backpack -> Lighter" — це різні записи.
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
                last_updated TIMESTAMP,
                CONSTRAINT unique_path UNIQUE(token, buy_exchange, sell_exchange)
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
                cursor.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                print(f"{C.RED}🧹 History CLEARED & WAL Truncated.{C.END}")
            except Exception as e:
                print(f"⚠️ Clean error: {e}")

    print(f"{C.GREEN}✅ Target DB Initialized.{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 📥 ЧИТАННЯ
# ═══════════════════════════════════════════════════════════════════════════

def get_data_from_source(db_config):
    db_path = os.path.join(DB_FOLDER, db_config['file'])
    if not os.path.exists(db_path): return None

    try:
        with closing(sqlite3.connect(db_path, timeout=10, isolation_level=None)) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
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

                time_diff = abs((buy_row['last_updated'] - sell_row['last_updated']).total_seconds())
                if time_diff > MAX_SYNC_DIFF_SEC: continue

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
# 💾 СТАТИСТИКА (ВИПРАВЛЕНО ГРУПУВАННЯ ПО ТОКЕНАХ)
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
                if IGNORE_NEGATIVE_HISTORY and row['spread'] <= MIN_HISTORY_SPREAD_THRESHOLD:
                    continue
                history_data.append((row['token'], row['route'], row['spread'], now_str))

            if history_data:
                cursor.executemany(
                    "INSERT INTO spread_history (token, route, spread_pct, timestamp) VALUES (?, ?, ?, ?)",
                    history_data
                )

            cursor.execute("DELETE FROM spread_history WHERE timestamp < datetime('now', '-30 days', '-1 hour')")
            conn.commit()

            elapsed_time = time.time() - SCRIPT_START_TIME

            if elapsed_time < STATS_WARMUP_SEC:
                for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                    df_live[col] = df_live['spread']
                return df_live

            else:
                # 🔥 ВИПРАВЛЕННЯ ТУТ:
                # Додаємо token у SELECT та GROUP BY
                stats_query = f"""
                SELECT 
                    token, 
                    route,
                    MIN(spread_pct) as db_min_24h,
                    MAX(spread_pct) as db_max_24h,
                    MIN(spread_pct) as db_min_30d,
                    MAX(spread_pct) as db_max_30d
                FROM spread_history
                WHERE timestamp >= datetime('now', '-24 hours')
                  AND spread_pct > {MIN_HISTORY_SPREAD_THRESHOLD} 
                GROUP BY token, route  -- 🔥 Групуємо по унікальній парі Токен+Маршрут
                """
                df_stats = pd.read_sql_query(stats_query, conn)

                if not df_stats.empty:
                    # 🔥 З'єднуємо таблиці по ДВОХ колонках: token і route
                    df_final = pd.merge(df_live, df_stats, on=['token', 'route'], how='left')

                    # Далі логіка залишається тією ж
                    df_final['min_24h'] = df_final['db_min_24h'].fillna(df_final['spread'])
                    df_final['max_24h'] = df_final['db_max_24h'].fillna(df_final['spread'])
                    df_final['min_30d'] = df_final['db_min_30d'].fillna(df_final['spread'])
                    df_final['max_30d'] = df_final['db_max_30d'].fillna(df_final['spread'])

                    def force_update_min(row, col_min):
                        current = row['spread']
                        history_min = row[col_min]
                        if current > MIN_HISTORY_SPREAD_THRESHOLD and current < history_min:
                            return current
                        return history_min

                    def force_update_max(row, col_max):
                        current = row['spread']
                        history_max = row[col_max]
                        if current > history_max:
                            return current
                        return history_max

                    df_final['min_24h'] = df_final.apply(lambda x: force_update_min(x, 'min_24h'), axis=1)
                    df_final['max_24h'] = df_final.apply(lambda x: force_update_max(x, 'max_24h'), axis=1)

                    df_final['min_30d'] = df_final.apply(lambda x: force_update_min(x, 'min_30d'), axis=1)
                    df_final['max_30d'] = df_final.apply(lambda x: force_update_max(x, 'max_30d'), axis=1)

                    return df_final
                else:
                    for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                        df_live[col] = df_live['spread']
                    return df_live

    except Exception as e:
        print(f"{C.RED}❌ Stats Error: {e}{C.END}")
        return df_live


# ═══════════════════════════════════════════════════════════════════════════
# 🔄 ОНОВЛЕННЯ БД (STATIC ID з правильним KEY)
# ═══════════════════════════════════════════════════════════════════════════

def update_dashboard_db(df_final):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")

            if not df_final.empty:
                data_to_insert = []
                for _, row in df_final.iterrows():
                    data_to_insert.append((
                        row['token'], row['route'], row['buy_exchange'], row['sell_exchange'],
                        row['buy_price'], row['sell_price'], row['spread'],
                        row['min_24h'], row['max_24h'], row['min_30d'], row['max_30d'],
                        row['net_funding'], row['funding_freq_str'],
                        row['oi_long'], row['oi_short'], row['vol_long'], row['vol_short'],
                        timestamp
                    ))

                # 🔥 ОНОВЛЕНО: ON CONFLICT(token, buy_exchange, sell_exchange)
                # Тепер оновлення прив'язане до конкретних бірж, а не до тексту маршруту.

                sql_upsert = '''
                    INSERT INTO live_opportunities 
                    (token, route, buy_exchange, sell_exchange, buy_price, sell_price, 
                    spread_pct, spread_min_24h, spread_max_24h, spread_min_30d, spread_max_30d,
                    net_funding_pct, funding_freq, 
                    oi_long_usd, oi_short_usd, vol_long_usd, vol_short_usd, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(token, buy_exchange, sell_exchange) DO UPDATE SET
                        route = excluded.route,  -- Навіть якщо текст маршруту зміниться, ми його оновимо
                        buy_price = excluded.buy_price,
                        sell_price = excluded.sell_price,
                        spread_pct = excluded.spread_pct,
                        spread_min_24h = excluded.spread_min_24h,
                        spread_max_24h = excluded.spread_max_24h,
                        spread_min_30d = excluded.spread_min_30d,
                        spread_max_30d = excluded.spread_max_30d,
                        net_funding_pct = excluded.net_funding_pct,
                        oi_long_usd = excluded.oi_long_usd,
                        oi_short_usd = excluded.oi_short_usd,
                        vol_long_usd = excluded.vol_long_usd,
                        vol_short_usd = excluded.vol_short_usd,
                        last_updated = excluded.last_updated
                '''
                cursor.executemany(sql_upsert, data_to_insert)

            # Видаляємо тільки мертве (старіше 1 години)
            cursor.execute("DELETE FROM live_opportunities WHERE last_updated < datetime('now', '-1 hour')")

            conn.commit()
            cursor.execute("PRAGMA wal_checkpoint(PASSIVE);")

    except Exception as e:
        print(f"{C.RED}❌ Write Error: {e}{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 ARBITRAGE AGGREGATOR (STRICT KEY & STATIC ID){C.END}")
    print(f"{C.YELLOW}Min Threshold: {MIN_HISTORY_SPREAD_THRESHOLD}% | Warmup: {STATS_WARMUP_SEC}s{C.END}")

    init_target_db()

    while True:
        start_time = time.time()
        dfs = []

        for db_conf in SOURCE_DBS:
            df = get_data_from_source(db_conf)
            if df is not None and not df.empty:
                dfs.append(df)

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
        print(f"\r{C.CYAN}[{ts}] Routes: {count}. Top: {top_spread:.2f}%. Took: {duration:.3f}s{C.END}", end="")

        time.sleep(PAUSE_AFTER_UPDATE)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{C.RED}🛑 Stopped.{C.END}")