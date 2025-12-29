import requests
import time


class BackpackEngine:
    def __init__(self):
        self.base_url = "https://api.backpack.exchange"

    def get_perp_symbols(self):
        """Отримуємо список ф'ючерсів, ігноруючи чорний список"""
        url = f"{self.base_url}/api/v1/markets"
        try:
            res = requests.get(url).json()
            BLACKLIST = ["TRX", "ORDER"]  # Можна розширювати
            return [
                m['symbol'] for m in res
                if m.get('marketType') == 'PERP'
                   and m['symbol'].split('_')[0] not in BLACKLIST
            ]
        except Exception as e:
            print(f"⚠️ Error getting symbols: {e}")
            return []

    def get_funding_rate(self, symbol):
        """
        НОВИЙ МЕТОД: Отримує останню історичну ставку фандінгу.
        Це найнадійніший спосіб отримати дані на Backpack.
        """
        url = f"{self.base_url}/api/v1/fundingRates?symbol={symbol}&limit=1"
        try:
            res = requests.get(url, timeout=2).json()

            if isinstance(res, list) and len(res) > 0:
                # Повертаємо ставку як float (вона приходить як рядок)
                return float(res[0].get('fundingRate', 0))
            return 0.0
        except Exception as e:
            # print(f"Funding error for {symbol}: {e}")
            return 0.0

    def get_depth(self, symbol):
        """Отримуємо стакан для визначення точних Bid/Ask"""
        url = f"{self.base_url}/api/v1/depth?symbol={symbol}&limit=5"
        try:
            res = requests.get(url, timeout=2).json()

            if not res.get('bids') or not res.get('asks'):
                return None

            # Вираховуємо найкращі ціни
            best_bid = max([float(x[0]) for x in res['bids']])
            best_ask = min([float(x[0]) for x in res['asks']])

            return {'bid': best_bid, 'ask': best_ask}
        except:
            return None