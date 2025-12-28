import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from paradex_py import ParadexSubkey


class ParadexEngine:
    def __init__(self):
        env_path = Path('data') / '.env'
        load_dotenv(dotenv_path=env_path)

        self.paradex = ParadexSubkey(
            env="prod",
            l2_private_key=os.getenv("PARADEX_SUBKEY"),
            l2_address=os.getenv("PARADEX_ACCOUNT_ADDRESS")
        )

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Авторизація
        self.loop.run_until_complete(self.paradex.init_account())

    def get_market_data(self):
        """
        Використовує метод fetch_markets_summary згідно з вашою документацією.
        """
        try:
            # Передаємо params={'market': 'ALL'} як вказано в доках
            res = self.loop.run_until_complete(
                self.paradex.api_client.fetch_markets_summary(params={'market': 'ALL'})
            )

            # Документація каже, що повертається словник з ключем 'results' (список)
            data = res.get('results', [])
            return {item['symbol']: item for item in data}
        except Exception as e:
            print(f"⚠️ Paradex SDK Error (fetch_markets_summary): {e}")
            return {}

    def get_order_book(self, symbol):
        """Отримує стакан для символу"""
        try:
            # Метод fetch_orderbook зазвичай очікує market як прямий аргумент
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
        """Отримує список всіх пар"""
        try:
            res = self.paradex.api_client.fetch_markets()
            markets = res.get('results', []) if isinstance(res, dict) else res
            return [m['symbol'] for m in markets if '-PERP' in m['symbol']]
        except:
            return []