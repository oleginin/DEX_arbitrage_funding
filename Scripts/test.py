import sys
import os

# Додаємо шлях до проекту
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine

# Стилізація
G, Y, B, X = "\033[92m", "\033[93m", "\033[1m", "\033[0m"


def list_tokens():
    print(f"{B}{Y}🔍 ПОШУК СПІЛЬНИХ ТОКЕНІВ...{X}\n")

    bp = BackpackEngine()
    pd = ParadexEngine()

    # Отримуємо сирі списки з кожної біржі
    bp_list = bp.get_perp_symbols()
    pd_list = pd.get_perp_symbols()

    # Співставляємо
    common = []
    for s in bp_list:
        token = s.split('_')[0]
        pd_name = f"{token}-USD-PERP"
        if pd_name in pd_list:
            common.append({
                'token': token,
                'bp_full': s,
                'pd_full': pd_name
            })

    # Вивід результатів у вигляді таблиці
    print(f"{B}{'#':<4} | {'TOKEN':<10} | {'BACKPACK SYMBOL':<18} | {'PARADEX SYMBOL':<18}{X}")
    print("-" * 65)

    for i, p in enumerate(common, 1):
        print(f"{i:<4} | {G}{p['token']:<10}{X} | {p['bp_full']:<18} | {p['pd_full']:<18}")

    print("-" * 65)
    print(f"\n{B}УСЬОГО СПІЛЬНИХ ПАР: {G}{len(common)}{X}")
    print(f"Backpack всього: {len(bp_list)} | Paradex всього: {len(pd_list)}")


if __name__ == "__main__":
    list_tokens()