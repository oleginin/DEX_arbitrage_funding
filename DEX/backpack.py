import requests


class BackpackEngine:
    def __init__(self):
        self.base_url = "https://api.backpack.exchange"

    def get_order_book(self, symbol):
        try:
            url = f"{self.base_url}/api/v1/depth?symbol={symbol}"
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                return {"error": f"BP Status {response.status_code}"}

            data = response.json()
            bids, asks = data.get('bids', []), data.get('asks', [])

            if not bids or not asks:
                return {"error": "BP Empty Book"}

            return {
                'bid': float(bids[0][0]),
                'ask': float(asks[0][0]),
                'error': None
            }
        except Exception as e:
            return {"error": f"BP Exception: {str(e)}"}