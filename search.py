import time
from DEX.backpack import BackpackEngine
from DEX.paradex import ParadexEngine

G, Y, C, B, R, X = "\033[92m", "\033[93m", "\033[96m", "\033[1m", "\033[91m", "\033[0m"


def display_info(token):
    bp = BackpackEngine()
    pd = ParadexEngine()

    bp_symbol = f"{token.upper()}_USDC_PERP"
    pd_symbol = f"{token.upper()}-USD-PERP"

    print(f"\n{B}{Y}🔍 ГЛИБОКИЙ АНАЛІЗ {token.upper()}...{X}")

    bp_all = bp.get_all_market_data()
    pd_all = pd.get_market_data()

    bp_data = bp_all.get(bp_symbol, {})
    pd_data = pd_all.get(pd_symbol, {})

    if not bp_data or not pd_data:
        print(f"{R}❌ Дані для {token.upper()} не знайдено.{X}")
        return

    # --- RAW DATA (Сирі дані) ---
    print(f"\n{B}--- RAW API DATA ---{X}")
    # Backpack дає 1h ставку
    print(f"Backpack RAW: {bp_data.get('fundingRate')}")
    # Paradex - перевіряємо всі можливі поля
    print(f"Paradex RAW (funding_rate):      {pd_data.get('funding_rate')}")
    print(f"Paradex RAW (last_funding_rate): {pd_data.get('last_funding_rate')}")

    f_bp_1h = float(bp_data.get('fundingRate', 0)) * 100
    f_pd_raw = float(pd_data.get('funding_rate', 0)) * 100

    print(f"\n{B}--- ПЕРЕВІРКА ГІПОТЕЗ (Paradex) ---{X}")
    print(f"Якщо це 1h: {f_pd_raw:.4f}%")
    print(f"Якщо це 8h (ділимо на 8): {f_pd_raw / 8:.4f}%")
    print(f"Якщо це 4h (ділимо на 4): {f_pd_raw / 4:.4f}%")

    # Ціни
    book_bp = bp.get_order_book(bp_symbol)
    book_pd = pd.get_order_book(pd_symbol)

    if book_bp and book_pd:
        # Використовуємо 1h для розрахунку (поки що без ділення, щоб порівняти)
        s_a = ((book_pd['bid'] - book_bp['ask']) / book_bp['ask']) * 100
        f_a = f_pd_raw - f_bp_1h  # ТУТ ПЕРЕВІРЯЄМО ЧИСТИЙ 1h

        print(f"\n{B}ПОТОЧНИЙ СТАН (без ділення):{X}")
        print(f"  Spread: {s_a:.3f}%")
        print(f"  Net Fund (1h): {f_a:.4f}%")
        print(f"  Score 24h: {s_a + (f_a * 24):.3f}%")


def main():
    while True:
        token = input(f"\nВведіть токен: ").strip()
        if token.lower() == 'exit': break
        display_info(token)


if __name__ == "__main__":
    main()