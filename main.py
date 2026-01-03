import sys
import os
import subprocess
import time
import io

# --- FIX WINDOWS ENCODING (ВИПРАВЛЕНО) ---
# Ми примусово ставимо UTF-8 тільки для ВИВОДУ (print), щоб малювались таблиці і емодзі.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ВАЖЛИВО: Ми ПРИБРАЛИ перекодування sys.stdin.
# Це дозволить Windows використовувати стандартне кодування для вводу з клавіатури
# і виправить помилку "0xff".

# --- CONFIG ---
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(ROOT_DIR, 'Scripts')

G, Y, B, R, X = "\033[92m", "\033[93m", "\033[1m", "\033[91m", "\033[0m"


def main_menu():
    while True:
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"{B}{G}╔══════════════════════════════════════════════════╗{X}")
        print(f"{B}{G}║            🤖 DEX ARBITRAGE & FUNDING BOT        ║{X}")
        print(f"{B}{G}╚══════════════════════════════════════════════════╝{X}")
        print("")
        print(" Оберіть режим роботи:")
        print(f"   [{B}1{X}] 📊 Відкрити DEX Моніторинг (Scanner)")
        print(f"   [{B}2{X}] 💸 Торгувати (Auto-Trade)")
        print(f"   [{B}3{X}] 🚪 Вихід")
        print("")

        try:
            # Тепер input() працює у стандартному режимі Windows
            choice = input(f" Ваш вибір > ").strip()
        except UnicodeDecodeError:
            print(f"\n{R}❌ Помилка кодування. Спробуйте ще раз.{X}")
            time.sleep(1)
            continue
        except EOFError:
            break
        except KeyboardInterrupt:
            print("\nBye.")
            sys.exit()

        if choice == '1':
            script_path = os.path.join(SCRIPTS_DIR, 'monitor_spread_fund.py')
            try:
                # check=False дозволяє скрипту завершитися без крашу головного меню
                subprocess.run([sys.executable, script_path], check=False)
            except KeyboardInterrupt:
                pass

        elif choice == '2':
            script_path = os.path.join(SCRIPTS_DIR, 'auto_trade.py')
            try:
                subprocess.run([sys.executable, script_path], check=False)
            except KeyboardInterrupt:
                pass

        elif choice == '3':
            print("👋 Bye!")
            sys.exit()

        else:
            print(f"{R}Невірний вибір.{X}")
            time.sleep(1)


if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        sys.exit()