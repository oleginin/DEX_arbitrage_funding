import time
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine

# Кольори
G, Y, B, R, X = "\033[92m", "\033[93m", "\033[1m", "\033[91m", "\033[0m"


def run_monitor():
    print(f"{B}{Y}🚀 МОНІТОРИНГ (ACCURATE FUNDING MODE){X}")
    bp, pd = BackpackEngine(), ParadexEngine()

    bp_list = bp.get_perp_symbols()
    pd_list = pd.get_perp_symbols()

    potential_pairs = []
    for s_bp in bp_list:
        token = s_bp.split('_')[0]
        s_pd = f"{token}-USD-PERP"
        if s_pd in pd_list:
            potential_pairs.append({'base': token, 'bp': s_bp, 'pd': s_pd})

    print(f"🔍 Знайдено {len(potential_pairs)} пар. Сканування може зайняти час...")
    print("-" * 155)
    print(
        f"{B}{'TOKEN':<7} | {'STRATEGY (Best Route)':<25} | {'PRICE BUY':<10} | {'PRICE SELL':<10} | {'BP 1h %':<9} | {'PD 1h %':<9} | {'SPREAD':>7} | {'SCORE 24h':>9}{X}")
    print("-" * 155)

    while True:
        try:
            # Оновлюємо дані Paradex (оптом)
            pd_all = pd.get_market_data()
            if not pd_all:
                time.sleep(1);
                continue

            for p in potential_pairs:
                # --- PARADEX ---
                pd_d = pd_all.get(p['pd'])
                if not pd_d: continue

                pd_bid = float(pd_d.get('bid', 0))
                pd_ask = float(pd_d.get('ask', 0))

                # Математика фандінгу Paradex: (8h rate / 8) * 100 = 1h %
                raw_pd_8h = float(pd_d.get('funding_rate', 0))
                f_pd_pct = (raw_pd_8h / 8) * 100

                # --- BACKPACK ---
                # 1. Фандінг (1h rate * 100 = 1h %)
                raw_bp = bp.get_funding_rate(p['bp'])
                f_bp_pct = raw_bp * 100

                # 2. Стакан (Точні ціни)
                bp_depth = bp.get_depth(p['bp'])
                if not bp_depth: continue
                bp_bid, bp_ask = bp_depth['bid'], bp_depth['ask']

                if 0 in [bp_bid, bp_ask, pd_bid, pd_ask]: continue

                # --- РОЗРАХУНОК ---

                # Варіант A: Long BP / Short PD
                spread_a = ((pd_bid - bp_ask) / bp_ask) * 100
                net_fund_a = f_pd_pct - f_bp_pct
                score_a = spread_a + (net_fund_a * 24)

                # Варіант B: Long PD / Short BP
                spread_b = ((bp_bid - pd_ask) / pd_ask) * 100
                net_fund_b = f_bp_pct - f_pd_pct
                score_b = spread_b + (net_fund_b * 24)

                # Вибір кращого
                if score_a > score_b:
                    direction_text = "Buy BP -> Sell PD"
                    color_dir = f"{G}Buy BP{X} -> {R}Sell PD{X}"

                    # ПРИСВОЮЄМО ПРАВИЛЬНІ ІМЕНА ЗМІННИМ
                    price_in = bp_ask
                    price_out = pd_bid

                    final_spread = spread_a
                    final_score = score_a
                    is_ideal = spread_a > 0 and net_fund_a > 0
                else:
                    direction_text = "Buy PD -> Sell BP"
                    color_dir = f"{G}Buy PD{X} -> {R}Sell BP{X}"

                    # ПРИСВОЮЄМО ПРАВИЛЬНІ ІМЕНА ЗМІННИМ
                    price_in = pd_ask
                    price_out = bp_bid

                    final_spread = spread_b
                    final_score = score_b
                    is_ideal = spread_b > 0 and net_fund_b > 0

                # Фільтр виводу (наприклад, Score > -1.0%)
                if final_score > -1.0:
                    score_col = G if final_score > 0 else X
                    fire = f"{Y}🔥{X}" if is_ideal else "  "

                    # Вирівнювання тексту напрямку
                    pad = " " * (25 - len(direction_text))
                    formatted_dir = f"{color_dir}{pad}"

                    # ВИВІД: ТУТ ТЕПЕР ВИКОРИСТОВУЄТЬСЯ price_in / price_out
                    # Також стоїть .5f для цін (5 знаків), щоб бачити спред
                    print(
                        f"{p['base']:<7} | {formatted_dir} | {price_in:<10.5f} | {price_out:<10.5f} | {f_bp_pct:>8.5f}% | {f_pd_pct:>8.5f}% | {final_spread:>6.2f}% | {score_col}{final_score:>8.2f}%{X} {fire}")

            time.sleep(120)

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}");
            time.sleep(5)


if __name__ == "__main__":
    run_monitor()