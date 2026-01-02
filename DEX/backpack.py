import requests
import time
import base64
import os
from cryptography.hazmat.primitives.asymmetric import ed25519


class BackpackEngine:
    def __init__(self):
        self.base_url = "https://api.backpack.exchange"

        self.api_key = os.getenv("BACKPACK_API_KEY")
        self.api_secret = os.getenv("BACKPACK_API_SECRET")

        self.private_key_obj = None

        if self.api_secret:
            try:
                self.private_key_obj = ed25519.Ed25519PrivateKey.from_private_bytes(
                    base64.b64decode(self.api_secret)
                )
            except Exception as e:
                print(f"⚠️ Backpack Key Error: {e}")

    # --- ДОПОМІЖНІ МЕТОДИ ---

    def _map_side(self, side):
        """Конвертує Buy/Sell у Bid/Ask"""
        s = side.lower()
        if s in ["buy", "long"]: return "Bid"
        if s in ["sell", "short"]: return "Ask"
        return side.capitalize()

    # --- АВТОРИЗАЦІЯ ---

    def _get_headers(self, instruction, params=None):
        if not self.private_key_obj or not self.api_key:
            return {}

        timestamp = int(time.time() * 1000)
        window = "5000"

        sign_str = f"instruction={instruction}"

        if params:
            sorted_params_list = []
            for key, value in sorted(params.items()):
                if isinstance(value, bool):
                    value = str(value).lower()
                sorted_params_list.append(f"{key}={value}")

            sorted_params = "&".join(sorted_params_list)
            if sorted_params:
                sign_str += "&" + sorted_params

        sign_str += f"&timestamp={timestamp}&window={window}"

        signature_bytes = self.private_key_obj.sign(sign_str.encode())
        encoded_signature = base64.b64encode(signature_bytes).decode()

        return {
            "X-API-Key": self.api_key,
            "X-Signature": encoded_signature,
            "X-Timestamp": str(timestamp),
            "X-Window": window,
            "Content-Type": "application/json; charset=utf-8",
        }

    # --- ПУБЛІЧНІ ДАНІ ---

    def get_perp_symbols(self):
        url = f"{self.base_url}/api/v1/markets"
        try:
            res = requests.get(url).json()
            BLACKLIST = ["TRX", "ORDER"]
            return [m['symbol'] for m in res if
                    m.get('marketType') == 'PERP' and m['symbol'].split('_')[0] not in BLACKLIST]
        except:
            return []

    def get_depth(self, symbol):
        url = f"{self.base_url}/api/v1/depth?symbol={symbol}&limit=5"
        try:
            res = requests.get(url, timeout=2).json()
            if not res.get('bids'): return None
            best_bid = max([float(x[0]) for x in res['bids']])
            best_ask = min([float(x[0]) for x in res['asks']])
            return {'bid': best_bid, 'ask': best_ask}
        except:
            return None

    def get_account_balance(self):
        """
        Отримує баланси, виводить список активів > 0 та рахує загальний еквівалент USD.
        """
        instruction = "balanceQuery"
        headers = self._get_headers(instruction)
        url = f"{self.base_url}/api/v1/capital"

        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                data = res.json()

                # --- ВІДОБРАЖЕННЯ ТА ПІДРАХУНОК ---

                total_usd_value = 0.0
                has_funds = False

                for asset, info in data.items():
                    # Конвертуємо стрінги в float
                    available = float(info.get('available', 0))
                    locked = float(info.get('locked', 0))
                    total_coin = available + locked

                    # Показуємо тільки якщо є гроші
                    if total_coin > 0:
                        has_funds = True


                        # Рахуємо загальний баланс у доларах (тільки стейбли)
                        # Якщо треба рахувати SOL в доларах, треба робити запит ціни,
                        # але для швидкості тут сумуємо стейбли.
                        if asset in ["USDC", "USDT", "USD"]:
                            total_usd_value += total_coin

                if not has_funds:
                    print("   ⚠️ Гаманець порожній.")

                print(f"   💵 BACKPACK БАЛАНС (USDT): ~{total_usd_value:.2f} $")
                print("=" * 30 + "\n")
                # ----------------------------------

                return data
            else:
                print(f"❌ Balance Error: {res.text}")
                return {}
        except Exception as e:
            print(f"⚠️ Balance Exception: {e}")
            return {}

    # --- ПОЗИЦІЇ (ОНОВЛЕНО ПІД ДОКУМЕНТАЦІЮ) ---

    def get_positions(self):
        """
        Отримує позиції.
        Endpoint: /api/v1/position (згідно вашої док)
        Instruction: positionQuery
        """
        instruction = "positionQuery"
        headers = self._get_headers(instruction)
        url = f"{self.base_url}/api/v1/position"  # <-- ВИПРАВЛЕНО

        try:
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                raw_data = res.json()
                # НОРМАЛІЗАЦІЯ ДАНИХ
                # Додаємо поле 'side' і 'liquidationPrice' для зручності
                cleaned_positions = []
                for p in raw_data:
                    net_qty = float(p.get('netQuantity', 0))

                    if net_qty == 0: continue  # Пропускаємо пусті

                    # Визначаємо сторону по знаку
                    side = "Long" if net_qty > 0 else "Short"

                    # Створюємо зручний об'єкт
                    cleaned_p = p.copy()
                    cleaned_p['side'] = side
                    cleaned_p['netQuantity'] = str(abs(net_qty))  # Беремо модуль числа для ордерів
                    cleaned_p['liquidationPrice'] = p.get('estLiquidationPrice', '0')  # Мапимо назву

                    cleaned_positions.append(cleaned_p)

                return cleaned_positions
            return []
        except Exception as e:
            print(f"⚠️ Get Positions Error: {e}")
            return []

    def get_open_orders(self, symbol=None):
        instruction = "orderQueryAll"
        params = {}
        if symbol: params["symbol"] = symbol
        headers = self._get_headers(instruction, params)
        url = f"{self.base_url}/api/v1/orders"
        try:
            res = requests.get(url, headers=headers, params=params)
            return res.json() if res.status_code == 200 else []
        except:
            return []

    # --- ТОРГІВЛЯ (EXECUTION) ---

    def place_limit_order(self, symbol, side, price, quantity):
        instruction = "orderExecute"
        api_side = self._map_side(side)

        params = {
            "symbol": symbol,
            "side": api_side,
            "orderType": "Limit",
            "price": str(price),
            "quantity": str(quantity),
            "timeInForce": "GTC"
        }

        headers = self._get_headers(instruction, params)
        url = f"{self.base_url}/api/v1/order"

        try:
            res = requests.post(url, headers=headers, json=params)
            if res.status_code == 200:
                return res.json().get('id') or res.json()
            print(f"❌ Limit Error: {res.text}")
            return None
        except Exception as e:
            print(f"❌ Limit Except: {e}")
            return None

    def place_market_order(self, symbol, side, quantity):
        instruction = "orderExecute"
        api_side = self._map_side(side)

        params = {
            "symbol": symbol,
            "side": api_side,
            "orderType": "Market",
            "quantity": str(quantity)
        }

        headers = self._get_headers(instruction, params)
        url = f"{self.base_url}/api/v1/order"

        try:
            res = requests.post(url, headers=headers, json=params)
            if res.status_code == 200:
                return res.json().get('id') or res.json()
            print(f"❌ Market Error: {res.text}")
            return None
        except Exception as e:
            print(f"❌ Market Except: {e}")
            return None

    def cancel_order(self, symbol, order_id):
        instruction = "orderCancel"
        params = {"symbol": symbol, "orderId": order_id}
        headers = self._get_headers(instruction, params)
        url = f"{self.base_url}/api/v1/order"
        try:
            res = requests.delete(url, headers=headers, json=params)
            return res.status_code == 200
        except:
            return False

    # --- ЗАКРИТТЯ ПОЗИЦІЙ ---

    def close_position_market(self, symbol):
        """Закриває всю позицію по ринку"""
        positions = self.get_positions()
        pos = next((p for p in positions if p['symbol'] == symbol), None)
        if not pos:
            print("⚠️ No position to close")
            return False

        # side вже визначено в get_positions (Long/Short)
        close_side = "Sell" if pos['side'] == "Long" else "Buy"
        qty = pos['netQuantity']  # Це вже модуль числа (позитивний)

        print(f"📉 Closing {pos['side']} (Market): {close_side} {qty}")
        return self.place_market_order(symbol, close_side, qty)

    def close_position_limit(self, symbol, price):
        """Закриває всю позицію ліміткою"""
        positions = self.get_positions()
        pos = next((p for p in positions if p['symbol'] == symbol), None)
        if not pos: return None

        close_side = "Sell" if pos['side'] == "Long" else "Buy"
        qty = pos['netQuantity']

        print(f"🧱 Closing {pos['side']} (Limit): {close_side} {qty} @ {price}")
        return self.place_limit_order(symbol, close_side, price, qty)

