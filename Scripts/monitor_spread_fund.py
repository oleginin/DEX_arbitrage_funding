import time
from datetime import datetime
from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine

# Стилізація
G, Y, C, B, R, X = "\033[92m", "\033[93m", "\033[96m", "\033[1m", "\033[91m", "\033[0m"


def run_monitor():
    print(f"{B}{Y}🚀 МОНІТОРИНГ ЗАПУЩЕНО...{X}")

    bp = BackpackEngine()
    pd = ParadexEngine()

    # 1. Початковий пошук спільних назв
    bp_list = bp.get_perp_symbols()
    pd_list = pd.get_perp_symbols()
    potential_pairs = []
    for s in bp_list:
        token = s.split('_')[0]
        pd_name = f"{token}-USD-PERP"
        if pd_name in pd_list:
            potential_pairs.append({'base': token, 'bp': s, 'pd': pd_name})

    # 2. ЕТАП ВАЛІДАЦІЇ: Видаляємо ті, що не дають даних (наприклад, TRX)
    print(f"{Y}🔍 Перевірка ліквідності та даних для {len(potential_pairs)} пар...{X}")

    bp_sample = bp.get_all_market_data()
    pd_sample = pd.get_market_data()

    common_pairs = []
    for p in potential_pairs:
        f_bp = bp_sample.get(p['bp'], {}).get('fundingRate')
        f_pd = pd_sample.get(p['pd'], {}).get('funding_rate')

        # Якщо обидві біржі дали бодай якесь число (навіть 0), залишаємо
        if f_bp is not None and f_pd is not None:
            common_pairs.append(p)
        else:
            # Якщо даних немає (як у випадку з TRX), монета просто не потрапляє в список
            pass

    print(f"{G}✅ Список очищено. Працюємо з {len(common_pairs)} живими парами.{X}")
    print("-" * 135)
    print(
        f"{B}{'Token':<8} | {'Напрямок':<18} | {'Spread %':<10} | {'F:BP 1h':<9} | {'F:PD 1h':<9} | {'Net Fund':<9} | {'Score 24h'}{X}")
    print("-" * 135)

    # 3. ОСНОВНИЙ ЦИКЛ (тільки з робочими монетами)
    while True:
        try:
            bp_all = bp.get_all_market_data()
            pd_all = pd.get_market_data()

            for p in common_pairs:
                # Дані точно є, бо ми перевірили їх вище
                f_bp = float(bp_all[p['bp']]['fundingRate']) * 100
                f_pd = (float(pd_all[p['pd']]['funding_rate']) * 100) / 4

                book_bp = bp.get_order_book(p['bp'])
                book_pd = pd.get_order_book(p['pd'])

                if not book_bp or not book_pd: continue

                # Стратегія А: Buy BP / Sell PD
                s_a = ((book_pd['bid'] - book_bp['ask']) / book_bp['ask']) * 100
                f_a = f_pd - f_bp
                score_a = s_a + (f_a * 24)

                # Стратегія Б: Buy PD / Sell BP
                s_b = ((book_bp['bid'] - book_pd['ask']) / book_pd['ask']) * 100
                f_b = f_bp - f_pd
                score_b = s_b + (f_b * 24)

                for spread, fund, score, direction in [(s_a, f_a, score_a, "L:BP ➔ S:PD"),
                                                       (s_b, f_b, score_b, "L:PD ➔ S:BP")]:
                    if score > 0.05:
                        color, label = X, ""
                        if spread > 0 and fund > 0:
                            color, label = G + B, " 🔥 [IDEAL]"
                        elif score > 0.4:
                            color, label = C, " 💰 [BEST]"

                        print(
                            f"{color}{p['base']:<8} | {direction:<18} | {spread:>9.3f}% | {f_bp:>8.4f}% | {f_pd:>8.4f}% | {fund:>8.4f}% | {score:>8.3f}%{label}{X}")

            time.sleep(10)
        except Exception as e:
            print(f"{R}⚠️ Помилка циклу: {e}{X}")
            time.sleep(5)