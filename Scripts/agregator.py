import sqlite3
import pandas as pd
import time
import os
from datetime import datetime, timedelta, timezone
from contextlib import closing

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ КОНФІГУРАЦІЯ
# ═══════════════════════════════════════════════════════════════════════════

PAUSE_AFTER_UPDATE = 15
RESET_HISTORY_ON_START = False
STATS_WARMUP_SEC = 60

HISTORY_RETENTION_DAYS = 8
NEW_TOKEN_GRACE_PERIOD_HOURS = 24

# 🔥 ПРИМУСОВЕ ОНОВЛЕННЯ (Якщо токен не оновлювався X секунд)
FORCE_UPDATE_TIMEOUT_SEC = 3600

# 🛑 ФІЛЬТРИ (Застосовуються тільки до "старих" токенів)
MIN_OI_USD = 50000
MIN_VOL_USD = 500000

# ⏰ СИНХРОНІЗАЦІЯ
MAX_DATA_DELAY_SEC = 60
MAX_SYNC_DIFF_SEC = 25

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

                buy_funding_24h_pct REAL DEFAULT 0,
                sell_funding_24h_pct REAL DEFAULT 0,

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
            CREATE TABLE IF NOT EXISTS funding_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT,
                token TEXT,
                funding_pct REAL,
                payout_time_utc TIMESTAMP,
                CONSTRAINT unique_payout UNIQUE(exchange, token, payout_time_utc)
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
        cursor.execute(
            'CREATE INDEX IF NOT EXISTS idx_fund_hist ON funding_history (exchange, token, payout_time_utc);')

        conn.commit()

        if RESET_HISTORY_ON_START:
            cursor.execute("DELETE FROM spread_history")
            cursor.execute("DELETE FROM funding_history")
            conn.commit()
            print(f"{C.RED}🧹 All History CLEARED.{C.END}")

    print(f"{C.GREEN}✅ Target DB Initialized.{C.END}")


# ═══════════════════════════════════════════════════════════════════════════
# 🕒 ОСТАННІ ОНОВЛЕННЯ (Для Force Update)
# ═══════════════════════════════════════════════════════════════════════════

def get_last_updated_map():
    """Повертає словник {token: datetime}, коли токен востаннє оновлювався на Дашборді."""
    last_updates = {}
    if not os.path.exists(TARGET_DB_PATH): return last_updates
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT token, MAX(last_updated) FROM live_opportunities GROUP BY token")
            for row in cursor.fetchall():
                if row[1]: last_updates[row[0]] = pd.to_datetime(row[1])
    except:
        pass
    return last_updates


# ═══════════════════════════════════════════════════════════════════════════
# 💰 ЛОГІКА ЗБЕРЕЖЕННЯ ФАНДІНГУ (UTC)
# ═══════════════════════════════════════════════════════════════════════════

def get_period_funding(row):
    try:
        rate = float(row['funding_pct'])
        freq = max(1, int(row['freq_hours']))
        return rate * freq if row['exchange'] == 'Variational' else rate
    except:
        return 0.0


def save_funding_snapshots(full_market_data):
    now_utc = datetime.now(timezone.utc)
    if now_utc.minute != 59: return

    next_hour = (now_utc.hour + 1) % 24
    payout_timestamp = (now_utc + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    payout_str = payout_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    funding_snapshots = []
    for _, row in full_market_data.iterrows():
        freq = max(1, int(row['freq_hours']))
        if next_hour % freq == 0:
            rate_for_period = get_period_funding(row)
            funding_snapshots.append((row['exchange'], row['token'], rate_for_period, payout_str))

    if funding_snapshots:
        try:
            with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
                cursor = conn.cursor()
                cursor.executemany('''
                    INSERT OR IGNORE INTO funding_history (exchange, token, funding_pct, payout_time_utc)
                    VALUES (?, ?, ?, ?)
                ''', funding_snapshots)
                cursor.execute(
                    f"DELETE FROM funding_history WHERE payout_time_utc < datetime('now', '-{HISTORY_RETENTION_DAYS} days')")
                conn.commit()
        except:
            pass


def get_24h_funding_stats():
    query = """
    SELECT exchange, token, SUM(funding_pct) as funding_24h
    FROM funding_history
    WHERE payout_time_utc >= datetime('now', '-24 hours')
    GROUP BY exchange, token
    """
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            return pd.read_sql_query(query, conn)
    except:
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# 📥 ЧИТАННЯ З ДЖЕРЕЛ ТА ІНШЕ
# ═══════════════════════════════════════════════════════════════════════════

def manage_new_tokens(current_tokens):
    if not current_tokens: return {}
    discovery_map = {}
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT token, discovery_time FROM token_discovery")
            known_tokens = {row[0]: pd.to_datetime(row[1]) for row in cursor.fetchall()}
            new_tokens = [(t, datetime.now().strftime('%Y-%m-%d %H:%M:%S')) for t in current_tokens if
                          t not in known_tokens]
            if new_tokens:
                cursor.executemany("INSERT OR IGNORE INTO token_discovery (token, discovery_time) VALUES (?, ?)",
                                   new_tokens)
                conn.commit()
                for t, _ in new_tokens: known_tokens[t] = datetime.now()
            discovery_map = known_tokens
    except:
        pass
    return discovery_map


def get_data_from_source(db_config):
    db_path = os.path.join(DB_FOLDER, db_config['file'])
    if not os.path.exists(db_path): return None
    try:
        with closing(sqlite3.connect(db_path, timeout=10, isolation_level=None)) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            df = pd.read_sql_query("SELECT * FROM market_data", conn)
            if df.empty: return None
            df.rename(columns={'funding_rate': 'funding_pct', 'fundingRate': 'funding_pct',
                               'predicted_funding_rate': 'funding_pct'}, inplace=True)
            df['last_updated'] = pd.to_datetime(df['last_updated'])
            fresh_df = df[df['last_updated'] > datetime.now() - timedelta(seconds=MAX_DATA_DELAY_SEC)].copy()
            if fresh_df.empty: return None
            fresh_df['exchange'] = db_config['name']
            if 'freq_hours' not in fresh_df.columns: fresh_df['freq_hours'] = 1
            return fresh_df
    except:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# 🧠 РОЗРАХУНОК (МАКЕР + FORCE UPDATE + FUNDING 24H)
# ═══════════════════════════════════════════════════════════════════════════

def calculate_live_routes(all_data_df, discovery_map, funding_24h_df, last_updated_map):
    if all_data_df.empty: return pd.DataFrame()
    results = []
    grouped = all_data_df.groupby('token')
    current_time = datetime.now()

    f24_map = {}
    if not funding_24h_df.empty:
        f24_map = funding_24h_df.set_index(['exchange', 'token'])['funding_24h'].to_dict()

    for token, group in grouped:
        if len(group) < 2: continue

        # 1. Перевірка на новий токен
        is_new_token = False
        if token in discovery_map:
            if (current_time - discovery_map[token]).total_seconds() / 3600 < NEW_TOKEN_GRACE_PERIOD_HOURS:
                is_new_token = True

        # 2. 🔥 ПЕРЕВІРКА НА FORCE UPDATE
        force_update = False
        if token in last_updated_map:
            if (current_time - last_updated_map[token]).total_seconds() > FORCE_UPDATE_TIMEOUT_SEC:
                force_update = True
        else:
            force_update = True

        # MAKER ЛОГІКА (Buy @ Bid, Sell @ Ask)
        potential_buys = group[group['bid'] > 0]
        potential_sells = group[group['ask'] > 0]

        for _, buy_row in potential_buys.iterrows():
            if not is_new_token and not force_update and (
                    buy_row['oi_usd'] < MIN_OI_USD or buy_row['volume_24h'] < MIN_VOL_USD): continue

            for _, sell_row in potential_sells.iterrows():
                if not is_new_token and not force_update and (
                        sell_row['oi_usd'] < MIN_OI_USD or sell_row['volume_24h'] < MIN_VOL_USD): continue
                if buy_row['exchange'] == sell_row['exchange']: continue

                time_diff = abs((buy_row['last_updated'] - sell_row['last_updated']).total_seconds())

                # Ігноруємо розсинхрон, якщо це Force Update
                if not force_update and time_diff > MAX_SYNC_DIFF_SEC: continue

                buy_price = buy_row['bid']
                sell_price = sell_row['ask']
                spread = ((sell_price - buy_price) / buy_price) * 100

                buy_fund_period = get_period_funding(buy_row)
                sell_fund_period = get_period_funding(sell_row)

                buy_fund_24h = f24_map.get((buy_row['exchange'], token), 0.0)
                sell_fund_24h = f24_map.get((sell_row['exchange'], token), 0.0)

                results.append({
                    'token': token,
                    'route': f"{buy_row['exchange']} ➡️ {sell_row['exchange']}",
                    'buy_exchange': buy_row['exchange'],
                    'sell_exchange': sell_row['exchange'],
                    'buy_price': buy_price,
                    'sell_price': sell_price,
                    'spread': spread,
                    'buy_funding_rate': buy_fund_period,
                    'buy_funding_freq': int(buy_row['freq_hours']),
                    'sell_funding_rate': sell_fund_period,
                    'sell_funding_freq': int(sell_row['freq_hours']),
                    'buy_funding_24h_pct': buy_fund_24h,
                    'sell_funding_24h_pct': sell_fund_24h,
                    'oi_long': buy_row['oi_usd'],
                    'oi_short': sell_row['oi_usd'],
                    'vol_long': buy_row['volume_24h'],
                    'vol_short': sell_row['volume_24h']
                })

    return pd.DataFrame(results)


# ═══════════════════════════════════════════════════════════════════════════
# ЗАПИС ТА MAIN
# ═══════════════════════════════════════════════════════════════════════════

def update_history_and_get_stats(df_live):
    if df_live.empty: return df_live
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            history_data = [(r['token'], r['route'], r['spread'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')) for _, r
                            in df_live.iterrows()]
            if history_data: cursor.executemany(
                "INSERT INTO spread_history (token, route, spread_pct, timestamp) VALUES (?, ?, ?, ?)", history_data)
            cursor.execute(
                f"DELETE FROM spread_history WHERE timestamp < datetime('now', '-{HISTORY_RETENTION_DAYS} days')")
            conn.commit()

            if time.time() - SCRIPT_START_TIME < STATS_WARMUP_SEC:
                for col in ['min_24h', 'max_24h', 'min_30d', 'max_30d']: df_live[col] = df_live['spread']
                return df_live
            else:
                df_stats = pd.read_sql_query("""
                SELECT token, route, MIN(CASE WHEN timestamp >= datetime('now', '-24 hours') THEN spread_pct END) as db_min_24h, MAX(CASE WHEN timestamp >= datetime('now', '-24 hours') THEN spread_pct END) as db_max_24h, MIN(spread_pct) as db_min_30d, MAX(spread_pct) as db_max_30d
                FROM spread_history GROUP BY token, route""", conn)
                if not df_stats.empty:
                    df_final = pd.merge(df_live, df_stats, on=['token', 'route'], how='left')
                    for col in ['db_min_24h', 'db_max_24h', 'db_min_30d', 'db_max_30d']: df_final[col] = df_final[
                        col].fillna(df_final['spread'])
                    df_final['min_24h'] = df_final[['spread', 'db_min_24h']].min(axis=1)
                    df_final['max_24h'] = df_final[['spread', 'db_max_24h']].max(axis=1)
                    df_final['min_30d'] = df_final[['spread', 'db_min_30d']].min(axis=1)
                    df_final['max_30d'] = df_final[['spread', 'db_max_30d']].max(axis=1)
                    return df_final
    except:
        pass
    return df_live


def update_dashboard_db(df_final):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        with closing(sqlite3.connect(TARGET_DB_PATH, timeout=10)) as conn:
            cursor = conn.cursor()
            if not df_final.empty:
                cols_to_round = ['buy_price', 'sell_price', 'spread', 'min_24h', 'max_24h', 'min_30d', 'max_30d',
                                 'buy_funding_rate', 'sell_funding_rate', 'buy_funding_24h_pct', 'sell_funding_24h_pct']
                for col in cols_to_round:
                    if col in df_final.columns: df_final[col] = df_final[col].round(5)

                data_to_insert = [(
                    r['token'], r['route'], r['buy_exchange'], r['sell_exchange'], r['buy_price'], r['sell_price'],
                    r['spread'],
                    r['min_24h'], r['max_24h'], r['min_30d'], r['max_30d'], r['buy_funding_rate'],
                    r['buy_funding_freq'],
                    r['sell_funding_rate'], r['sell_funding_freq'], r['buy_funding_24h_pct'], r['sell_funding_24h_pct'],
                    r['oi_long'], r['oi_short'], r['vol_long'], r['vol_short'], timestamp
                ) for _, r in df_final.iterrows()]

                cursor.executemany('''
                    INSERT INTO live_opportunities (token, route, buy_exchange, sell_exchange, buy_price, sell_price, spread_pct, spread_min_24h, spread_max_24h, spread_min_30d, spread_max_30d, buy_funding_rate, buy_funding_freq, sell_funding_rate, sell_funding_freq, buy_funding_24h_pct, sell_funding_24h_pct, oi_long_usd, oi_short_usd, vol_long_usd, vol_short_usd, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(token, buy_exchange, sell_exchange) DO UPDATE SET route=excluded.route, buy_price=excluded.buy_price, sell_price=excluded.sell_price, spread_pct=excluded.spread_pct, spread_min_24h=excluded.spread_min_24h, spread_max_24h=excluded.spread_max_24h, spread_min_30d=excluded.spread_min_30d, spread_max_30d=excluded.spread_max_30d, buy_funding_rate=excluded.buy_funding_rate, buy_funding_freq=excluded.buy_funding_freq, sell_funding_rate=excluded.sell_funding_rate, sell_funding_freq=excluded.sell_funding_freq, buy_funding_24h_pct=excluded.buy_funding_24h_pct, sell_funding_24h_pct=excluded.sell_funding_24h_pct, oi_long_usd=excluded.oi_long_usd, oi_short_usd=excluded.oi_short_usd, vol_long_usd=excluded.vol_long_usd, vol_short_usd=excluded.vol_short_usd, last_updated=excluded.last_updated
                ''', data_to_insert)

            cursor.execute("DELETE FROM live_opportunities WHERE last_updated < datetime('now', '-5 minute')")
            conn.commit()
    except Exception as e:
        print(f"{C.RED}❌ DB Write Error: {e}{C.END}")


def main():
    print(f"\n{C.CYAN}🚀 ARBITRAGE AGGREGATOR{C.END}")
    print(f"{C.GREEN}Feature: 24h Funding Tracker & 2-Min Force Update active.{C.END}")
    init_target_db()

    while True:
        start_time = time.time()
        dfs = [get_data_from_source(db) for db in SOURCE_DBS if
               get_data_from_source(db) is not None and not get_data_from_source(db).empty]

        if not dfs:
            print(f"\r{C.RED}⚠️ Waiting for FRESH data...{C.END}", end="")
            time.sleep(1)
            continue

        full_market_data = pd.concat(dfs, ignore_index=True)

        save_funding_snapshots(full_market_data)
        funding_24h_df = get_24h_funding_stats()
        discovery_map = manage_new_tokens(full_market_data['token'].unique().tolist())

        # 🔥 Отримуємо таймери оновлення
        last_updated_map = get_last_updated_map()

        # 🔥 Передаємо всі дані в розрахунок
        df_live = calculate_live_routes(full_market_data, discovery_map, funding_24h_df, last_updated_map)
        df_final = update_history_and_get_stats(df_live)

        if not df_final.empty: df_final = df_final.sort_values(by='spread', ascending=False)
        update_dashboard_db(df_final)

        ts = datetime.now().strftime('%H:%M:%S')
        print(f"\r{C.CYAN}[{ts}] Routes: {len(df_final)}. Took: {time.time() - start_time:.3f}s{C.END}", end="")
        time.sleep(PAUSE_AFTER_UPDATE)


if __name__ == "__main__":
    main()