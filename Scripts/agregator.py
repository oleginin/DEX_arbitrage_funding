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
RESET_HISTORY_ON_START = False
STATS_WARMUP_SEC = 60

# 📅 ІСТОРІЯ
HISTORY_RETENTION_DAYS = 8

# 👶 НОВІ ТОКЕНИ
NEW_TOKEN_GRACE_PERIOD_HOURS = 24

# 🛑 ФІЛЬТРИ (Застосовуються тільки до "старих" токенів)
MIN_OI_USD = 50000
MIN_VOL_USD = 500000

# ⏰ СИНХРОНІЗАЦІЯ
MAX_DATA_DELAY_SEC = 60
MAX_SYNC_DIFF_SEC = 25

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
# 🛠️ РОБОТА З БАЗОЮ ДАНИХ (TARGET DB)
# ═══════════════════════════════════════════════════════════════════════════

def init_target_db():
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)

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

                buy_funding_rate REAL,
                buy_funding_freq INTEGER,
                sell_funding_rate REAL,
                sell_funding_freq INTEGER,

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

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS token_discovery (
                token TEXT PRIMARY KEY,
                discovery_time TIMESTAMP
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hist_token_route ON spread_history (token, route);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_hist_time ON spread_history (timestamp);')

        conn.commit()

        if RESET_HISTORY_ON_START:
            cursor.execute("DELETE FROM spread_history")
            conn.commit()
            print(f"{C.RED}🧹 History CLEARED.{C.END}")

    print(f"{C.GREEN}✅ Target DB Initialized.{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 👶 УПРАВЛІННЯ НОВИМИ ТОКЕНАМИ
# ═══════════════════════════════════════════════════════════════════════════

def manage_new_tokens(current_tokens):
    if not current_tokens: return {}
    discovery_map = {}
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT token, discovery_time FROM token_discovery")
            rows = cursor.fetchall()
            known_tokens = {row[0]: pd.to_datetime(row[1]) for row in rows}

            new_tokens = []
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            for t in current_tokens:
                if t not in known_tokens:
                    new_tokens.append((t, now_str))
                    known_tokens[t] = datetime.now()

            if new_tokens:
                cursor.executemany("INSERT OR IGNORE INTO token_discovery (token, discovery_time) VALUES (?, ?)",
                                   new_tokens)
                conn.commit()
                print(f"{C.YELLOW}👶 Discovered {len(new_tokens)} new tokens! Filters disabled for 24h.{C.END}")
            discovery_map = known_tokens
    except Exception as e:
        print(f"{C.RED}❌ Token Discovery Error: {e}{C.END}")
    return discovery_map


# ═══════════════════════════════════════════════════════════════════════════
# 📥 ЧИТАННЯ З ДЖЕРЕЛ
# ═══════════════════════════════════════════════════════════════════════════

def get_data_from_source(db_config):
    db_path = os.path.join(DB_FOLDER, db_config['file'])
    if not os.path.exists(db_path): return None

    try:
        with closing(sqlite3.connect(db_path, timeout=10, isolation_level=None)) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            query = "SELECT * FROM market_data"
            df = pd.read_sql_query(query, conn)

            if df.empty: return None

            df.rename(columns={
                'funding_rate': 'funding_pct',
                'fundingRate': 'funding_pct',
                'predicted_funding_rate': 'funding_pct'
            }, inplace=True)

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

def get_period_funding(row):
    """
    Рахує реальний фандінг за період.
    """
    try:
        rate = float(row['funding_pct'])
        freq = max(1, int(row['freq_hours']))

        # 🔥 ВИПРАВЛЕНО: Множимо ТІЛЬКИ для Variational
        if row['exchange'] == 'Variational':
            return rate * freq
        else:
            return rate
    except:
        return 0.0


def calculate_live_routes(all_data_df, discovery_map):
    if all_data_df.empty: return pd.DataFrame()
    results = []
    grouped = all_data_df.groupby('token')
    current_time = datetime.now()

    for token, group in grouped:
        if len(group) < 2: continue

        is_new_token = False
        if token in discovery_map:
            discovery_time = discovery_map[token]
            hours_since_discovery = (current_time - discovery_time).total_seconds() / 3600
            if hours_since_discovery < NEW_TOKEN_GRACE_PERIOD_HOURS:
                is_new_token = True

        potential_buys = group[group['ask'] > 0]
        potential_sells = group[group['bid'] > 0]

        for _, buy_row in potential_buys.iterrows():
            if not is_new_token:
                if buy_row['oi_usd'] < MIN_OI_USD or buy_row['volume_24h'] < MIN_VOL_USD: continue

            for _, sell_row in potential_sells.iterrows():
                if not is_new_token:
                    if sell_row['oi_usd'] < MIN_OI_USD or sell_row['volume_24h'] < MIN_VOL_USD: continue

                if buy_row['exchange'] == sell_row['exchange']: continue

                time_diff = abs((buy_row['last_updated'] - sell_row['last_updated']).total_seconds())
                if time_diff > MAX_SYNC_DIFF_SEC: continue

                buy_price = buy_row['ask']
                sell_price = sell_row['bid']

                spread = ((sell_price - buy_price) / buy_price) * 100

                route_name = f"{buy_row['exchange']} ➡️ {sell_row['exchange']}"

                buy_fund_period = get_period_funding(buy_row)
                sell_fund_period = get_period_funding(sell_row)

                results.append({
                    'token': token,
                    'route': route_name,
                    'buy_exchange': buy_row['exchange'],
                    'sell_exchange': sell_row['exchange'],
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'spread': spread,
                    'buy_funding_rate': buy_fund_period,
                    'buy_funding_freq': int(buy_row['freq_hours']),
                    'sell_funding_rate': sell_fund_period,
                    'sell_funding_freq': int(sell_row['freq_hours']),
                    'oi_long': buy_row['oi_usd'],
                    'oi_short': sell_row['oi_usd'],
                    'vol_long': buy_row['volume_24h'],
                    'vol_short': sell_row['volume_24h']
                })

    return pd.DataFrame(results)


# ═══════════════════════════════════════════════════════════════════════════
# 💾 СТАТИСТИКА
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

            if history_data:
                cursor.executemany(
                    "INSERT INTO spread_history (token, route, spread_pct, timestamp) VALUES (?, ?, ?, ?)",
                    history_data
                )

            clean_query = f"DELETE FROM spread_history WHERE timestamp < datetime('now', '-{HISTORY_RETENTION_DAYS} days')"
            cursor.execute(clean_query)
            conn.commit()

            elapsed_time = time.time() - SCRIPT_START_TIME

            if elapsed_time < STATS_WARMUP_SEC:
                for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                    df_live[col] = df_live['spread']
                return df_live
            else:
                stats_query_full = """
                SELECT 
                    token, 
                    route,
                    MIN(CASE WHEN timestamp >= datetime('now', '-24 hours') THEN spread_pct END) as db_min_24h,
                    MAX(CASE WHEN timestamp >= datetime('now', '-24 hours') THEN spread_pct END) as db_max_24h,
                    MIN(spread_pct) as db_min_30d, 
                    MAX(spread_pct) as db_max_30d
                FROM spread_history
                GROUP BY token, route
                """

                df_stats = pd.read_sql_query(stats_query_full, conn)

                if not df_stats.empty:
                    df_final = pd.merge(df_live, df_stats, on=['token', 'route'], how='left')

                    for col in ['db_min_24h', 'db_max_24h', 'db_min_30d', 'db_max_30d']:
                        df_final[col] = df_final[col].fillna(df_final['spread'])

                    df_final['min_24h'] = df_final[['spread', 'db_min_24h']].min(axis=1)
                    df_final['max_24h'] = df_final[['spread', 'db_max_24h']].max(axis=1)
                    df_final['min_30d'] = df_final[['spread', 'db_min_30d']].min(axis=1)
                    df_final['max_30d'] = df_final[['spread', 'db_max_30d']].max(axis=1)

                    return df_final
                else:
                    for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']:
                        df_live[col] = df_live['spread']
                    return df_live

    except Exception as e:
        print(f"{C.RED}❌ Stats Error: {e}{C.END}")
        return df_live


def update_dashboard_db(df_final):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN TRANSACTION")

            if not df_final.empty:
                cols_to_round = [
                    'buy_price', 'sell_price', 'spread',
                    'min_24h', 'max_24h', 'min_30d', 'max_30d',
                    'buy_funding_rate', 'sell_funding_rate'
                ]
                for col in cols_to_round:
                    if col in df_final.columns:
                        df_final[col] = df_final[col].round(5)

                data_to_insert = []
                for _, row in df_final.iterrows():
                    data_to_insert.append((
                        row['token'], row['route'], row['buy_exchange'], row['sell_exchange'],
                        row['buy_price'], row['sell_price'], row['spread'],
                        row['min_24h'], row['max_24h'], row['min_30d'], row['max_30d'],
                        row['buy_funding_rate'], row['buy_funding_freq'],
                        row['sell_funding_rate'], row['sell_funding_freq'],
                        row['oi_long'], row['oi_short'], row['vol_long'], row['vol_short'],
                        timestamp
                    ))

                sql_upsert = '''
                    INSERT INTO live_opportunities 
                    (token, route, buy_exchange, sell_exchange, buy_price, sell_price, 
                    spread_pct, spread_min_24h, spread_max_24h, spread_min_30d, spread_max_30d,
                    buy_funding_rate, buy_funding_freq, sell_funding_rate, sell_funding_freq,
                    oi_long_usd, oi_short_usd, vol_long_usd, vol_short_usd, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                    ON CONFLICT(token, buy_exchange, sell_exchange) DO UPDATE SET
                        route = excluded.route,
                        buy_price = excluded.buy_price,
                        sell_price = excluded.sell_price,
                        spread_pct = excluded.spread_pct,
                        spread_min_24h = excluded.spread_min_24h,
                        spread_max_24h = excluded.spread_max_24h,
                        spread_min_30d = excluded.spread_min_30d,
                        spread_max_30d = excluded.spread_max_30d,
                        buy_funding_rate = excluded.buy_funding_rate,
                        buy_funding_freq = excluded.buy_funding_freq,
                        sell_funding_rate = excluded.sell_funding_rate,
                        sell_funding_freq = excluded.sell_funding_freq,
                        oi_long_usd = excluded.oi_long_usd,
                        oi_short_usd = excluded.oi_short_usd,
                        vol_long_usd = excluded.vol_long_usd,
                        vol_short_usd = excluded.vol_short_usd,
                        last_updated = excluded.last_updated
                '''
                cursor.executemany(sql_upsert, data_to_insert)

            cursor.execute("DELETE FROM live_opportunities WHERE last_updated < datetime('now', '-1 hour')")
            conn.commit()

    except Exception as e:
        print(f"{C.RED}❌ Write Error: {e}{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{C.CYAN}🚀 ARBITRAGE AGGREGATOR{C.END}")
    print(f"{C.YELLOW}Feature: New tokens bypass filters for {NEW_TOKEN_GRACE_PERIOD_HOURS}h.{C.END}")
    print(f"{C.YELLOW}Fix: Paradex funding taken AS-IS. Variational multiplied by freq.{C.END}")

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

        current_tokens_list = full_market_data['token'].unique().tolist()
        discovery_map = manage_new_tokens(current_tokens_list)

        df_live = calculate_live_routes(full_market_data, discovery_map)
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