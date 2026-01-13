import multiprocessing
import time
import sys
import os

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ НАЛАШТУВАННЯ ШЛЯХІВ
# ═══════════════════════════════════════════════════════════════════════════

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MONITORS_DIR = os.path.join(CURRENT_DIR, 'Dex_monitor')

if MONITORS_DIR not in sys.path:
    sys.path.append(MONITORS_DIR)

try:
    import backpack_monitor
    import paradex_monitor
    import variational_monitor
    import extended_monitor
    import lighter_monitor
except ImportError as e:
    print(f"❌ Error importing monitors: {e}")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 ГОЛОВНИЙ КЛАС
# ═══════════════════════════════════════════════════════════════════════════

class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


def run_monitor(target_func, name):
    """Обгортка для запуску процесу."""
    try:
        target_func()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        # Логуємо помилку, щоб знати, чому процес впав
        print(f"{C.RED}❌ Process {name} crashed: {e}{C.END}")


if __name__ == "__main__":
    multiprocessing.freeze_support()

    print(f"\n{C.BOLD}{C.CYAN}🚀 LAUNCHING ALL EXCHANGE MONITORS...{C.END}")

    # Список: (Функція, Назва, Час останнього падіння для захисту від спаму)
    # Третій елемент 0 — це timestamp останнього рестарту
    monitors_config = [
        {"func": backpack_monitor.main, "name": "Backpack", "last_restart": 0},
        {"func": paradex_monitor.main, "name": "Paradex", "last_restart": 0},
        {"func": variational_monitor.main, "name": "Variational", "last_restart": 0},
        {"func": extended_monitor.main, "name": "Extended", "last_restart": 0},
        {"func": lighter_monitor.main, "name": "Lighter (WSS)", "last_restart": 0}
    ]

    processes = [None] * len(monitors_config)


    # Функція для запуску конкретного процесу за індексом
    def start_process(index):
        cfg = monitors_config[index]
        p = multiprocessing.Process(target=run_monitor, args=(cfg["func"], cfg["name"]))
        p.start()
        processes[index] = p
        print(f"{C.GREEN}✅ Started: {cfg['name']} (PID: {p.pid}){C.END}")
        return p


    # Первинний запуск
    for i in range(len(monitors_config)):
        start_process(i)
        time.sleep(0.5)  # Невелика пауза між стартами, щоб не пікувати CPU

    print(f"\n{C.YELLOW}⚡ All systems active. CPU Monitor: optimized.{C.END}")
    print(f"{C.YELLOW}🛑 Press Ctrl+C to stop.{C.END}\n")

    try:
        while True:
            # 1. Збільшуємо інтервал перевірки.
            # Головному процесу достатньо прокидатися раз на 5-10 секунд.
            time.sleep(5)

            for i, p in enumerate(processes):
                if not p.is_alive():
                    cfg = monitors_config[i]
                    name = cfg["name"]

                    # 2. Логіка захисту від швидкого перезапуску (Backoff)
                    current_time = time.time()
                    if current_time - cfg["last_restart"] < 10:
                        # Якщо впав швидше ніж за 10 секунд після старту, чекаємо
                        print(f"{C.RED}⚠️ {name} keeps crashing. Waiting before restart...{C.END}")
                        time.sleep(5)

                    print(f"{C.YELLOW}🔄 Restarting {name}...{C.END}")
                    monitors_config[i]["last_restart"] = time.time()
                    start_process(i)

    except KeyboardInterrupt:
        print(f"\n{C.RED}🛑 STOPPING ALL MONITORS...{C.END}")
        for p in processes:
            if p and p.is_alive():
                p.terminate()
                p.join()
        print(f"{C.GREEN}✅ Done.{C.END}")