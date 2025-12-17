from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine
import time


def main():
    bp = BackpackEngine()
    pd = ParadexEngine()

    # Встановлюємо правильні символи
    symbol_bp = "SOL_USDC_PERP"
    symbol_pd = "SOL-USD-PERP"

    print(f"🔄 Моніторинг запущено...")

    while True:
        res_bp = bp.get_order_book(symbol_bp)
        res_pd = pd.get_order_book(symbol_pd)

        # Якщо є помилка — виводимо її детально
        if res_bp.get('error') or res_pd.get('error'):
            print(f"⚠️ Помилка мережі або символу:")
            if res_bp.get('error'): print(f"   Backpack: {res_bp['error']}")
            if res_pd.get('error'): print(f"   Paradex: {res_pd['error']}")
        else:
            # Розрахунок спреду (якщо дані є)
            spread = ((res_pd['bid'] - res_bp['ask']) / res_bp['ask']) * 100

            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"BP: {res_bp['ask']:.2f} | PD: {res_pd['bid']:.2f} | "
                  f"Spread: {spread:.4f}%")

        time.sleep(2)


if __name__ == "__main__":
    main()