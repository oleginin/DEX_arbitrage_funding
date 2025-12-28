import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from paradex_py import ParadexSubkey


class ParadexEngine:
    def __init__(self):
        # ВИПРАВЛЕННЯ: Динамічне визначення шляху до кореня проекту
        # __file__ - це шлях до цього файлу (paradex.py)
        # .parent - папка DEX
        # .parent.parent - корінь проекту
        root_dir = Path(__file__).resolve().parent.parent
        env_path = root_dir / 'data' / '.env'

        if not env_path.exists():
            print(f"⚠️ Файл .env не знайдено за шляхом: {env_path}")

        load_dotenv(dotenv_path=env_path)

        l2_key = os.getenv("PARADEX_SUBKEY")
        l2_addr = os.getenv("PARADEX_ACCOUNT_ADDRESS")

        if not l2_key:
            # Це саме те місце, де виникала ваша помилка
            raise ValueError(f"Ключ PARADEX_SUBKEY порожній. Перевірте файл: {env_path}")

        self.paradex = ParadexSubkey(
            env="prod",
            l2_private_key=l2_key,
            l2_address=l2_addr
        )

        # Ініціалізація акаунту (синхронна обгортка для асинхронного методу)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        loop.run_until_complete(self.paradex.init_account())

    def get_market_data(self):
        """
        Синхронний виклик fetch_markets_summary.
        """
        try:
            # Викликаємо НАПРЯМУ, без loop.run_until_complete
            res = self.paradex.api_client.fetch_markets_summary(params={'market': 'ALL'})

            # Перевіряємо структуру відповіді
            if isinstance(res, dict) and 'results' in res:
                data = res['results']
                return {item['symbol']: item for item in data}
            return {}
        except Exception as e:
            print(f"⚠️ Paradex Error (Market Data): {e}")
            return {}

    def get_order_book(self, symbol):
        """Синхронне отримання стакана"""
        try:
            book = self.paradex.api_client.fetch_orderbook(market=symbol)
            if not book or 'bids' not in book or not book['bids']:
                return None
            return {
                'bid': float(book['bids'][0][0]),
                'ask': float(book['asks'][0][0])
            }
        except:
            return None

    def get_perp_symbols(self):
        """Синхронне отримання списку маркетів"""
        try:
            res = self.paradex.api_client.fetch_markets()
            markets = res.get('results', []) if isinstance(res, dict) else res
            return [m['symbol'] for m in markets if '-PERP' in m['symbol']]
        except:
            return []