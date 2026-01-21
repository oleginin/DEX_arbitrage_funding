import multiprocessing
import time
import sys
import os
import subprocess

# ═══════════════════════════════════════════════════════════════════════════
# ⚙️ НАЛАШТУВАННЯ ШЛЯХІВ
# ═══════════════════════════════════════════════════════════════════════════

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
MONITORS_DIR = os.path.join(CURRENT_DIR, 'Dex_monitor')
SCRIPTS_DIR = os.path.join(CURRENT_DIR, 'Scripts')  # <--- Додали шлях до Scripts

# Додаємо шляхи, щоб Python бачив файли в цих папках
if MONITORS_DIR not in sys.path:
    sys.path.append(MONITORS_DIR)
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)
if CURRENT_DIR not in sys.path:
    sys.path.append(CURRENT_DIR)

try:
    # --- Імпорт Моніторів ---
    import backpack_monitor
    import paradex_monitor
    import variational_monitor
    import extended_monitor
    import lighter_monitor

    # --- Імпорт Агрегатора ---
    # Тепер Python знайде його, бо ми додали SCRIPTS_DIR у sys.path
    import agregator

except ImportError as e:
    print(f"❌ Error importing modules: {e}")
    print(f"🔍 Checked paths: {sys.path}")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
# 🚀 ДОДАТКОВІ ФУНКЦІЇ ЗАПУСКУ
# ═══════════════════════════════════════════════════════════════════════════

class C:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'


def run_monitor(target_func, name):
    """Обгортка для запуску звичайних Python функцій (Монітори, Агрегатор)."""
    try:
        target_func()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"{C.RED}❌ Process {name} crashed: {e}{C.END}")


def run_dashboard_process():
    """Обгортка для запуску Streamlit Dashboard."""
    # Припускаємо, що dashboard.py лежить в КОРЕНІ (там де main.py)
    dashboard_path = os.path.join(CURRENT_DIR, 'dashboard.py')

    if not os.path.exists(dashboard_path):
        print(f"{C.RED}❌ Dashboard file not found at: {dashboard_path}{C.END}")
        return

    # Команда: python -m streamlit run dashboard.py
    cmd = [
        sys.executable, "-m", "streamlit", "run", dashboard_path,
        "--server.port", "8501",
        "--server.headless", "true",
        "--server.address", "0.0.0.0",
        "--theme.base", "dark"  # Можна задати темну тему примусово
    ]

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"{C.RED}❌ Dashboard crashed with exit code {e.returncode}{C.END}")
    except KeyboardInterrupt:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# 🏁 MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Важливо для Windows
    multiprocessing.freeze_support()

    print(f"\n{C.BOLD}{C.CYAN}🚀 LAUNCHING RIDDLE ARBITRAGE SYSTEM...{C.END}")

    # Список процесів
    processes_config = [
        # --- МОНІТОРИ (в Dex_monitor) ---
        {"func": backpack_monitor.main, "name": "Backpack", "is_streamlit": False, "last_restart": 0},
        {"func": paradex_monitor.main, "name": "Paradex", "is_streamlit": False, "last_restart": 0},
        {"func": variational_monitor.main, "name": "Variational", "is_streamlit": False, "last_restart": 0},
        {"func": extended_monitor.main, "name": "Extended", "is_streamlit": False, "last_restart": 0},
        {"func": lighter_monitor.main, "name": "Lighter (WSS)", "is_streamlit": False, "last_restart": 0},

        # --- АГРЕГАТОР (в Scripts) ---
        {"func": agregator.main, "name": "agregator", "is_streamlit": False, "last_restart": 0},

        # --- DASHBOARD (в корені) ---
        {"func": run_dashboard_process, "name": "Dashboard UI", "is_streamlit": True, "last_restart": 0}
    ]

    active_processes = [None] * len(processes_config)


    def start_process(index):
        cfg = processes_config[index]

        if cfg["is_streamlit"]:
            p = multiprocessing.Process(target=cfg["func"], name=cfg["name"])
        else:
            p = multiprocessing.Process(target=run_monitor, args=(cfg["func"], cfg["name"]), name=cfg["name"])

        p.start()
        active_processes[index] = p

        # Іконки для краси
        if "agregator" in cfg["name"]:
            icon = "🧠"
        elif "Dashboard" in cfg["name"]:
            icon = "📊"
        else:
            icon = "📡"

        print(f"{C.GREEN}✅ Started: {icon} {cfg['name']} (PID: {p.pid}){C.END}")
        return p


    # 🔥 ЗАПУСК ВСІХ ПРОЦЕСІВ
    for i in range(len(processes_config)):
        start_process(i)

    print(f"\n{C.YELLOW}⚡ System operational.{C.END}")
    print(f"{C.YELLOW}📊 Dashboard: http://localhost:8501{C.END}")
    print(f"{C.YELLOW}🛑 Press Ctrl+C to stop.{C.END}\n")

    try:
        while True:
            time.sleep(5)

            for i, p in enumerate(active_processes):
                if not p.is_alive():
                    cfg = processes_config[i]
                    name = cfg["name"]

                    current_time = time.time()
                    if current_time - cfg["last_restart"] < 10:
                        print(f"{C.RED}⚠️ {name} keeps crashing. Waiting 5s...{C.END}")
                        time.sleep(5)

                    print(f"{C.YELLOW}🔄 Restarting {name}...{C.END}")
                    processes_config[i]["last_restart"] = time.time()
                    start_process(i)

    except KeyboardInterrupt:
        print(f"\n{C.RED}🛑 SHUTTING DOWN...{C.END}")
        for p in active_processes:
            if p and p.is_alive():
                p.terminate()
                p.join()
        print(f"{C.GREEN}✅ Done.{C.END}")