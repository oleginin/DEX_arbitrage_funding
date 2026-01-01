import time
import sys
import os
import itertools
from pathlib import Path
from dotenv import load_dotenv

# --- НАЛАШТУВАННЯ ШЛЯХІВ ---
ROOT_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = ROOT_DIR / 'data' / '.env'

if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    load_dotenv()

sys.path.append(str(ROOT_DIR))

from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine
from DEX.ethereal import EtherealEngine

# Кольори
G, Y, B, R, X = "\033[92m", "\033[93m", "\033[1m", "\033[91m", "\033[0m"
C = "\033[96m"

# Мапа назв
DEX_KEY_MAP = {
    "ETHEREAL": "ETHER",
    "PARADEX": "PARAD",
    "BACKPACK": "BACKP"
}


def safe_float(v):
    try:
        return float(v) if v else 0.0
    except:
        return 0.0


def parse_list_env(env_var_name):
    """Зчитує рядок з .env і перетворює в список (BTC, ETH -> ['BTC', 'ETH'])"""
    raw = os.getenv(env_var_name, "")
    if not raw: return []
    # Розбиваємо по комі, видаляємо пробіли, переводимо в UpperCase
    return [x.strip().upper() for x in raw.split(',') if x.strip()]


def run_monitor():
    # 1. Читаємо конфіг
    raw_main_dex = os.getenv("MAIN_DEX", "ALL").upper().replace(" ", "")
    target_dex_list = raw_main_dex.split(',')

    # Режими бірж
    is_all_mode = 'ALL' in target_dex_list
    is_anchor_mode = (len(target_dex_list) == 1) and not is_all_mode
    is_exclusive_mode = (len(target_dex_list) > 1) and not is_all_mode

    try:
        min_spread = float(os.getenv("MIN_SPREAD", "0"))
    except:
        min_spread = 0.0

    # 2. Зчитуємо списки фільтрації
    whitelist = parse_list_env("WHITELIST")
    blacklist = parse_list_env("BLACKLIST")

    print(f"{B}{Y}🚀 MULTI-DEX MONITOR{X}")
    print(f"🔧 DEX CONFIG: {raw_main_dex}")
    print(f"📉 MIN SPREAD: {min_spread}%")

    # Відображення активного фільтру
    if whitelist:
        print(f"{G}✅ WHITELIST ACTIVE: Торгуємо тільки {whitelist}{X}")
    elif blacklist:
        print(f"{R}⛔ BLACKLIST ACTIVE: Ігноруємо {blacklist}{X}")
    else:
        print(f"{C}🌊 NO FILTERS: Торгуємо всім доступним{X}")

    # Ініціалізація
    bp = BackpackEngine()
    pd = ParadexEngine()
    eth = EtherealEngine()

    header = f"{B}{'ASSET':<8} | {'STRATEGY (Long -> Short)':<30} | {'P_LONG':<10} | {'P_SHORT':<10} | {'F_LONG %':<9} | {'F_SHRT %':<9} | {'SPREAD':>7} | {'F_DIFF/1h':>8} | {'SCORE/24h':>8}{X}"

    while True:
        try:
            print("-" * 175)
            print(header)
            print("-" * 175)

            # --- ЗБІР ДАНИХ ---
            eth_data = eth.get_market_data()
            pd_data = pd.get_market_data()

            bp_symbols = bp.get_perp_symbols()
            bp_map = {s.split('_')[0]: s for s in bp_symbols if "_USDC_PERP" in s}

            # --- ФОРМУВАННЯ ПОВНОГО СПИСКУ ---
            all_tokens_set = set(eth_data.keys()) | set(pd_data.keys()) | set(bp_map.keys())

            # 1. Фільтрація по Anchor Mode (якщо вибрана одна головна біржа)
            if is_anchor_mode:
                anchor_name = target_dex_list[0]
                if anchor_name == "ETHEREAL":
                    all_tokens_set &= eth_data.keys()
                elif anchor_name == "PARADEX":
                    all_tokens_set &= pd_data.keys()
                elif anchor_name == "BACKPACK":
                    all_tokens_set &= bp_map.keys()

            # 2. Фільтрація WHITELIST / BLACKLIST (Core Logic)
            final_tokens_list = []

            if whitelist:
                # Якщо є Whitelist - беремо тільки те, що є в Whitelist і на ринку
                final_tokens_list = [t for t in all_tokens_set if t in whitelist]

            elif blacklist:
                # Якщо Whitelist нема, але є Blacklist - беремо все, чого нема в Blacklist
                final_tokens_list = [t for t in all_tokens_set if t not in blacklist]

            else:
                # Якщо нічого нема - беремо все
                final_tokens_list = list(all_tokens_set)

            found = 0

            # --- АНАЛІЗ ---
            for base in sorted(final_tokens_list):
                markets = {}

                if base in eth_data: markets['ETHER'] = eth_data[base]
                if base in pd_data: markets['PARAD'] = pd_data[base]

                # Backpack
                if base in bp_map:
                    should_fetch_bp = False
                    if len(markets) > 0: should_fetch_bp = True
                    if is_exclusive_mode and "BACKPACK" in target_dex_list: should_fetch_bp = True

                    if should_fetch_bp:
                        bp_sym = bp_map[base]
                        d = bp.get_depth(bp_sym)
                        if d:
                            markets['BACKP'] = {
                                'bid': d['bid'], 'ask': d['ask'],
                                'funding_pct': bp.get_funding_rate(bp_sym) * 100
                            }

                # Визначаємо доступні пари
                available_keys = list(markets.keys())
                valid_pairs = []

                if is_all_mode:
                    valid_pairs = list(itertools.combinations(available_keys, 2))
                elif is_anchor_mode:
                    anchor_key = DEX_KEY_MAP.get(target_dex_list[0])
                    if anchor_key in available_keys:
                        for other in available_keys:
                            if other != anchor_key: valid_pairs.append((anchor_key, other))
                elif is_exclusive_mode:
                    allowed_keys = [DEX_KEY_MAP.get(name) for name in target_dex_list]
                    allowed_keys = [k for k in allowed_keys if k]
                    current_valid_keys = [k for k in available_keys if k in allowed_keys]
                    if len(current_valid_keys) >= 2:
                        valid_pairs = list(itertools.combinations(current_valid_keys, 2))

                # Розрахунок
                for dA, dB in valid_pairs:
                    mA, mB = markets[dA], markets[dB]

                    # 1. Long A -> Short B
                    spread1 = ((mB['bid'] - mA['ask']) / mA['ask']) * 100
                    if spread1 >= min_spread:
                        net1 = mB['funding_pct'] - mA['funding_pct']
                        score1 = spread1 + (net1 * 24)
                        _print(base, dA, dB, mA['ask'], mB['bid'], mA['funding_pct'], mB['funding_pct'], spread1, net1,
                               score1)
                        found += 1

                    # 2. Long B -> Short A
                    spread2 = ((mA['bid'] - mB['ask']) / mB['ask']) * 100
                    if spread2 >= min_spread:
                        net2 = mA['funding_pct'] - mB['funding_pct']
                        score2 = spread2 + (net2 * 24)
                        _print(base, dB, dA, mB['ask'], mA['bid'], mB['funding_pct'], mA['funding_pct'], spread2, net2,
                               score2)
                        found += 1

            print(f"\n{Y}Оновлення... (Знайдено: {found}){X}", end="\r")
            time.sleep(3)
            os.system('cls' if os.name == 'nt' else 'clear')

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)


def _print(base, l_dex, s_dex, p_l, p_s, f_l, f_s, spr, net, sc):
    if spr > 50: return
    sc_col = G if sc > 0 else X
    nf_col = G if net > 0 else R
    fire = f"{Y}🔥{X}" if (spr > 0.1 and sc > 0.5) else "  "
    as_col = C if "ETHER" in [l_dex, s_dex] else B
    d_str = f"Buy {l_dex} -> Sell {s_dex}"
    col_str = f"{G}Buy {l_dex}{X} -> {R}Sell {s_dex}{X}"
    pad = " " * (30 - len(d_str))
    print(
        f"{as_col}{base:<8}{X} | {col_str}{pad} | {p_l:<10.5f} | {p_s:<10.5f} | {f_l:>8.5f}% | {f_s:>8.5f}% | {spr:>6.2f}% | {nf_col}{net:>7.4f}%{X} | {sc_col}{sc:>8.2f}%{X} {fire}")
