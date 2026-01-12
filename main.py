import sys
import os
import subprocess
import time
import io

# --- FIX WINDOWS ENCODING ---
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(ROOT_DIR, 'Scripts')
Dex_DIR = os.path.join(ROOT_DIR, 'Dex_monitor')

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
        print(f"   [{B}2{X}] 💸 Відкрити Backpack Monitor")
        print(f"   [{B}3{X}] 🚪 Вихід")
        print("")

        try:
            choice = input(f" Ваш вибір > ").strip()
        except UnicodeDecodeError:
            continue
        except EOFError:
            break
        except KeyboardInterrupt:
            sys.exit()

        if choice == '1':
            script_path = os.path.join(Dex_DIR, 'monitor_spread_fund.py')
            try:
                # ДОДАНО '-u' - це вимикає буферизацію Python
                subprocess.run([sys.executable, '-u', script_path], check=False)
            except KeyboardInterrupt:
                pass

        elif choice == '2':
            script_path = os.path.join(SCRIPTS_DIR, 'auto_trade.py')
            try:
                # ДОДАНО '-u'
                subprocess.run([sys.executable, '-u', script_path], check=False)
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