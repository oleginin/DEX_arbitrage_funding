import sys
import os

# Додаємо шлях до проекту, щоб Python бачив усі папки
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Scripts.monitor_spread_fund import run_monitor

if __name__ == "__main__":
    try:
        run_monitor()
    except KeyboardInterrupt:
        print("\n🛑 Роботу завершено.")
    except Exception as e:
        print(f"❌ Критична помилка при запуску: {e}")