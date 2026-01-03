import time
import sys
import os
import itertools
import concurrent.futures
import io
from pathlib import Path
from dotenv import load_dotenv

# --- FIX WINDOWS ENCODING & BUFFERING (ВАЖЛИВО) ---
# line_buffering=True змушує виводити текст миттєво
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', line_buffering=True)
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', line_buffering=True)

# --- НАЛАШТУВАННЯ ШЛЯХІВ ---
ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / 'data' / '.env'

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    load_dotenv()

sys.path.append(str(ROOT_DIR))

# --- ІМПОРТИ ---
try:
    from DEX.backpack import BackpackEngine
    from DEX.paradex import ParadexEngine

    try:
        from DEX.ethereal import EtherealEngine
    except ImportError:
        EtherealEngine = None
except ImportError as e:
    print(f"❌ Critical Error: {e}")
    sys.exit()

G, Y, B, R, X = "\033[92m", "\033[93m", "\033[1m", "\033[91m", "\033[0m"
C = "\033[96m"
DEX_KEY_MAP = {"ETHEREAL": "ETHER", "PARADEX": "PARAD", "BACKPACK": "BACKP"}


def parse_list_env(env_var_name):
    raw = os.getenv(env_var_name, "")
    if not raw: return []
    return [x.strip().upper() for x in raw.split(',') if x.strip()]


def get_backpack_full_data(engine, symbol, perp_symbol):
    try:
        depth = engine.get_depth(perp_symbol)
        if not depth: return None
        funding = engine.get_funding_rate(perp_symbol)
        return {'bid': depth['bid'], 'ask': depth['ask'], 'funding_pct': funding * 100}
    except:
        return None


def fetch_backpack_parallel_full(engine, symbols_map, executor):
    results = {}
    if not symbols_map: return results
    future_to_base = {
        executor.submit(get_backpack_full_data, engine, base, sym): base
        for base, sym in symbols_map.items()
    }
    for future in concurrent.futures.as_completed(future_to_base):
        base = future_to_base[future]
        try:
            data = future.result()
            if data: results[base] = data
        except:
            pass
    return results


def run_monitor():
    # --- 1. ЗАВАНТАЖЕННЯ КОНФІГУ ---
    raw_main_dex = os.getenv("MAIN_DEX", "ALL").upper().replace(" ", "")
    target_dex_list = raw_main_dex.split(',')
    is_all_mode = 'ALL' in target_dex_list
    is_anchor_mode = (len(target_dex_list) == 1) and not is_all_mode
    is_exclusive_mode = (len(target_dex_list) > 1) and not is_all_mode

    try:
        min_spread = float(os.getenv("MIN_SPREAD", "0"))
    except:
        min_spread = 0.0

    try:
        refresh_interval = float(os.getenv("REFRESH_INTERVAL", "1.0"))
    except:
        refresh_interval = 1.0

    try:
        eth_timeout = float(os.getenv("ETH_TIMEOUT", "5"))
    except:
        eth_timeout = 5.0

    try:
        pd_timeout = float(os.getenv("PD_TIMEOUT", "10"))
    except:
        pd_timeout = 10.0

    try:
        max_workers_bp = max(1, int(os.getenv("BP_WORKERS", "40")))
    except:
        max_workers_bp = 40

    whitelist = parse_list_env("WHITELIST")
    blacklist = parse_list_env("BLACKLIST")

    # --- 2. ВИВІД ІНФОРМАЦІЇ ПРО КОНФІГ ---
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"{B}{G}╔══════════════════════════════════════════════════╗{X}")
    print(f"{B}{G}║          ⚙️  MONITOR CONFIGURATION               ║{X}")
    print(f"{B}{G}╚══════════════════════════════════════════════════╝{X}")

    print(f" {B}• Exchanges (MAIN_DEX):{X} {C}{raw_main_dex}{X}")
    print(f" {B}• Min Spread Filter:{X}    {C}{min_spread}%{X}")
    print(f" {B}• Refresh Interval:{X}    {C}{refresh_interval}s{X}")

    wl_str = ", ".join(whitelist) if whitelist else "OFF (Trading All)"
    print(f" {B}• Whitelist:{X}            {Y}{wl_str}{X}")

    bl_str = ", ".join(blacklist) if blacklist else "None"
    print(f" {B}• Blacklist:{X}            {R}{bl_str}{X}")

    print("-" * 52)
    print(f"{Y}🚀 INITIALIZING ENGINES... (Please wait){X}")
    # Примусовий злив буфера, щоб текст з'явився відразу
    sys.stdout.flush()

    # --- 3. ІНІЦІАЛІЗАЦІЯ ДВИГУНІВ ---
    bp = None;
    pd = None;
    eth = None
    try:
        try:
            bp = BackpackEngine()
        except Exception as e:
            print(f"{R}Backpack Init Error: {e}{X}")
        try:
            pd = ParadexEngine()
        except Exception as e:
            print(f"{R}Paradex Init Error: {e}{X}")
        if EtherealEngine:
            try:
                eth = EtherealEngine()
            except:
                pass
    except:
        return

    if not bp:
        print(f"{R}❌ Backpack failed. Stop.{X}")
        return

    # Кеш символів та нормалізація
    bp_symbols_raw = bp.get_perp_symbols()
    bp_map = {s.split('_')[0].strip().upper(): s for s in bp_symbols_raw if "_USDC_PERP" in s}

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers_bp)

    header = f"{B}{'ASSET':<8} | {'STRATEGY (Maker L -> Maker S)':<30} | {'BID (L)':<10} | {'ASK (S)':<10} | {'F_LONG %':<9} | {'F_SHRT %':<9} | {'SPREAD':>7} | {'F_DIFF':>8} | {'SCORE':>8}{X}"

    # --- 4. ГОЛОВНИЙ ЦИКЛ ---
    while True:
        start_time = time.time()
        try:
            # А. ЗАПИТ ДАНИХ
            future_eth = None;
            future_pd = None
            if eth: future_eth = executor.submit(eth.get_market_data)
            if pd and getattr(pd, 'paradex', None): future_pd = executor.submit(pd.get_market_data)

            tokens_to_fetch_bp = {}
            if whitelist:
                tokens_to_fetch_bp = {k: v for k, v in bp_map.items() if k in whitelist}
            elif blacklist:
                tokens_to_fetch_bp = {k: v for k, v in bp_map.items() if k not in blacklist}
            elif is_all_mode or "BACKPACK" in target_dex_list:
                tokens_to_fetch_bp = bp_map

            bp_data = fetch_backpack_parallel_full(bp, tokens_to_fetch_bp, executor)

            eth_data = {}
            if future_eth:
                try:
                    eth_data = future_eth.result(timeout=eth_timeout)
                except:
                    pass

            pd_data = {}
            if future_pd:
                try:
                    raw_pd = future_pd.result(timeout=pd_timeout)
                    for k, v in raw_pd.items():
                        pd_data[k.strip().upper()] = v
                except:
                    pass

            # Б. ВІДОБРАЖЕННЯ
            os.system('cls' if os.name == 'nt' else 'clear')

            # --- БЛОК ІНФОРМАЦІЇ ПРО КОНФІГ ---
            print(f"{B}╔══════════════════════════════════════════════════════════════════════════════════════════╗{X}")
            print(
                f"{B}║ ⚙️  CONFIG | DEX: {C}{raw_main_dex:<10}{X} {B}| Min Spread: {C}{min_spread}%{X}                      {B}║{X}")

            if whitelist:
                wl_str = ", ".join(whitelist[:5]) + ("..." if len(whitelist) > 5 else "")
                print(f"{B}║ 🎯 WhiteList: {Y}{wl_str:<70}{X} {B}║{X}")
            elif blacklist:
                bl_str = ", ".join(blacklist[:5]) + ("..." if len(blacklist) > 5 else "")
                print(f"{B}║ ⛔ BlackList: {R}{bl_str:<70}{X} {B}║{X}")

            print(f"{B}╚══════════════════════════════════════════════════════════════════════════════════════════╝{X}")

            print(header)
            print("-" * 155)

            # Фільтрація токенів
            pd_keys = set(pd_data.keys())
            all_tokens = set(eth_data.keys()) | pd_keys | set(bp_map.keys())

            if is_anchor_mode:
                anchor = target_dex_list[0]
                if anchor == "ETHEREAL":
                    all_tokens &= eth_data.keys()
                elif anchor == "PARADEX":
                    all_tokens &= pd_keys
                elif anchor == "BACKPACK":
                    all_tokens &= bp_map.keys()

            final_tokens = sorted(list(all_tokens))
            if whitelist:
                final_tokens = [t for t in final_tokens if t in whitelist]
            elif blacklist:
                final_tokens = [t for t in final_tokens if t not in blacklist]

            found = 0

            # В. РОЗРАХУНОК
            for base in final_tokens:
                markets = {}
                if base in eth_data: markets['ETHER'] = eth_data[base]
                if base in pd_data: markets['PARAD'] = pd_data[base]
                if base in bp_data: markets['BACKP'] = bp_data[base]

                if len(markets) < 2: continue

                keys = list(markets.keys())
                valid_pairs = []

                if is_all_mode:
                    valid_pairs = list(itertools.combinations(keys, 2))
                elif is_anchor_mode:
                    anc = DEX_KEY_MAP.get(target_dex_list[0])
                    if anc in keys:
                        for o in keys:
                            if o != anc: valid_pairs.append((anc, o))
                elif is_exclusive_mode:
                    allowed = [DEX_KEY_MAP.get(n) for n in target_dex_list]
                    curr = [k for k in keys if k in allowed]
                    if len(curr) >= 2: valid_pairs = list(itertools.combinations(curr, 2))

                for dA, dB in valid_pairs:
                    mA, mB = markets[dA], markets[dB]

                    if mA['bid'] <= 0 or mB['ask'] <= 0 or mB['bid'] <= 0 or mA['ask'] <= 0:
                        continue

                    fA = mA.get('funding_pct', 0.0)
                    fB = mB.get('funding_pct', 0.0)

                    # 1. Long A -> Short B
                    spread1 = ((mB['ask'] - mA['bid']) / mA['bid']) * 100
                    if spread1 >= min_spread:
                        net1 = fB - fA
                        score1 = spread1 + (net1 * 24)
                        _print(base, dA, dB, mA['bid'], mB['ask'], fA, fB, spread1, net1, score1)
                        found += 1

                    # 2. Long B -> Short A
                    spread2 = ((mA['ask'] - mB['bid']) / mB['bid']) * 100
                    if spread2 >= min_spread:
                        net2 = fA - fB
                        score2 = spread2 + (net2 * 24)
                        _print(base, dB, dA, mB['bid'], mA['ask'], fB, fA, spread2, net2, score2)
                        found += 1

            loop_time = time.time() - start_time
            print("-" * 155)
            print(f"{Y}⚡ Loop: {loop_time:.2f}s | Found: {found}{X}")
            print(f"{C}   (Press Ctrl+C to stop){X}")

            # Примусовий злив, щоб таблиця з'явилась точно
            sys.stdout.flush()
            sleep_for = max(0.0, refresh_interval - loop_time)
            if sleep_for > 0:
                time.sleep(sleep_for)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Loop Error: {e}")
            time.sleep(2)


def _print(base, l_dex, s_dex, p_l, p_s, f_l, f_s, spr, net, sc):
    sc_col = G if sc > 0 else X
    nf_col = G if net > 0 else R
    fire = f"{Y}🔥{X}" if (spr > 0.2 and sc > 1.0) else "  "
    as_col = C if "ETHER" in [l_dex, s_dex] else B
    col_str = f"{G}L:{l_dex}{X} -> {R}S:{s_dex}{X}"
    pad = " " * (30 - len(f"L:{l_dex} -> S:{s_dex}"))
    print(
        f"{as_col}{base:<8}{X} | {col_str}{pad} | {p_l:<10.5f} | {p_s:<10.5f} | {f_l:>8.5f}% | {f_s:>8.5f}% | {spr:>6.2f}% | {nf_col}{net:>7.4f}%{X} | {sc_col}{sc:>8.2f}%{X} {fire}")


if __name__ == "__main__":
    run_monitor()