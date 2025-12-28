import requests

class BackpackEngine:
    def __init__(self):
        self.base_url = "https://api.backpack.exchange"

    def get_perp_symbols(self):
        try:
            url = f"{self.base_url}/api/v1/markets"
            res = requests.get(url, timeout=5).json()
            return [m['symbol'] for m in res if '_PERP' in m['symbol']]
        except:
            return []

    def get_all_market_data(self):
        """Отримує фандінг та ціни марка для всіх пар"""
        try:
            url = f"{self.base_url}/api/v1/markPrices"
            res = requests.get(url, timeout=5).json()
            # Робимо словник для швидкого пошуку
            return {item['symbol']: item for item in res}
        except:
            return {}

    def get_order_book(self, symbol):
        try:
            url = f"{self.base_url}/api/v1/depth?symbol={symbol}"
            res = requests.get(url, timeout=2).json()
            bids, asks = res.get('bids', []), res.get('asks', [])
            if not bids or not asks: return None
            return {
                'bid': max(float(b[0]) for b in bids),
                'ask': min(float(a[0]) for a in asks)
            }
        except:
            return None