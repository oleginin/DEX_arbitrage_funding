import os
import asyncio
import time
from pathlib import Path
from dotenv import load_dotenv
from paradex_py import ParadexSubkey


class ParadexEngine:
    def __init__(self):

        # 1. Шляхи до .env
        root_dir = Path(__file__).resolve().parent.parent
        env_path = root_dir / 'data' / '.env'

        if not env_path.exists():
            print(f"⚠️ Paradex Error: Файл .env не знайдено за шляхом: {env_path}")

        load_dotenv(dotenv_path=env_path)

        # 2. Отримання ключів
        l2_key = os.getenv("PARADEX_SUBKEY")
        l2_addr = os.getenv("PARADEX_ACCOUNT_ADDRESS")

        if not l2_key or not l2_addr:
            raise ValueError(f"❌ Ключі PARADEX відсутні в .env! Перевірте PARADEX_SUBKEY та PARADEX_ACCOUNT_ADDRESS")

        # 3. Створення клієнта
        try:
            self.paradex = ParadexSubkey(
                env="prod",
                l2_private_key=l2_key,
                l2_address=l2_addr
            )
        except Exception as e:
            print(f"❌ Paradex Client Error: {e}")
            raise e



        # Створюємо або отримуємо цикл подій для синхронного запуску
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Запускаємо init_account і ЧЕКАЄМО завершення
        try:
            self.loop.run_until_complete(self.paradex.init_account())

        except Exception as e:
            print(f"❌ Paradex Init Failed: {e}")
            # Не кидаємо помилку, щоб не вбити весь бот, але Paradex не працюватиме
            self.paradex = None

    def get_market_data(self):
        """
        Отримуємо дані для монітора.
        Цей метод викликається синхронно з monitor_spread_fund.py
        """
        if not self.paradex:
            return {}

        try:
            # Виконуємо запит через цикл, щоб отримати дані, а не корутину
            # fetch_markets_summary зазвичай асинхронний у цій бібліотеці
            task = self.paradex.api_client.fetch_markets_summary(params={'market': 'ALL'})

            # Якщо метод повертає корутину, чекаємо її виконання
            if asyncio.iscoroutine(task):
                res = self.loop.run_until_complete(task)
            else:
                res = task  # Якщо раптом бібліотека синхронна

            # Обробка результату
            results = res.get('results', []) if isinstance(res, dict) else res

            data = {}
            for item in results:
                symbol = item['symbol']
                if not symbol.endswith("-USD-PERP"):
                    continue

                base = symbol.split('-')[0]

                # --- Парсинг (адаптація під монітор) ---
                # Шукаємо найкращі ціни
                bid = float(item.get('best_bid', item.get('bid', 0)) or 0)
                ask = float(item.get('best_ask', item.get('ask', 0)) or 0)

                # Фандінг (8h -> 1h)
                raw_fund = float(item.get('funding_rate', 0) or 0)
                fund_1h_pct = (raw_fund * 100) / 8

                data[base] = {
                    'bid': bid,
                    'ask': ask,
                    'funding_pct': fund_1h_pct
                }
            return data

        except Exception as e:
            print(f"⚠️ Paradex Data Error: {e}")
            return {}