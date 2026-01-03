import os
import asyncio
import time
from pathlib import Path
from decimal import Decimal
from dotenv import load_dotenv

# Імпорти бібліотеки
try:
    from paradex_py import ParadexSubkey
    from paradex_py.environment import Environment
    from paradex_py.common.order import Order, OrderType, OrderSide
except ImportError:
    print("❌ Error: Library 'paradex-py' not found. Install via: pip install paradex-py")


class ParadexEngine:
    def __init__(self):
        # 1. Шляхи до .env
        root_dir = Path(__file__).resolve().parent.parent
        env_path = root_dir / 'data' / '.env'
        load_dotenv(dotenv_path=env_path)

        # 2. Отримання ключів
        l2_key = os.getenv("PARADEX_SUBKEY")
        l2_addr = os.getenv("PARADEX_ACCOUNT_ADDRESS")

        if not l2_key or not l2_addr:
            print("❌ Ключі PARADEX відсутні в .env!")
            self.paradex = None
            return

        # 3. Створення циклу подій (Event Loop)
        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # 4. Створення та ініціалізація клієнта
        try:
            self.paradex = ParadexSubkey(
                env="prod",
                l2_private_key=l2_key,
                l2_address=l2_addr
            )

            # Запускаємо init_account синхронно через loop
            self._run_async(self.paradex.init_account())
            print(f"✅ Paradex Initialized ({l2_addr[:6]}...)")

        except Exception as e:
            print(f"❌ Paradex Init Failed: {e}")
            self.paradex = None

    def _run_async(self, coro):
        """Магічний метод: запускає асинхронну функцію синхронно"""
        return self.loop.run_until_complete(coro)

    def _map_symbol(self, symbol):
        """Backpack (SOL_USDC_PERP) -> Paradex (SOL-USD-PERP)"""
        if "-USD-PERP" in symbol: return symbol
        return symbol.replace("_", "-").replace("USDC", "USD")

    # --- МЕТОД ДЛЯ МОНІТОРА (ВАШ РОБОЧИЙ КОД) ---

    def get_market_data(self):
        """
        Отримуємо дані для монітора.
        """
        if not self.paradex: return {}

        try:
            # Виконуємо запит через цикл
            task = self.paradex.api_client.fetch_markets_summary(params={'market': 'ALL'})

            # Обробка результату (чи це корутина, чи ні)
            if asyncio.iscoroutine(task):
                res = self._run_async(task)
            else:
                res = task

            results = res.get('results', []) if isinstance(res, dict) else res
            data = {}

            for item in results:
                symbol = item['symbol']
                if not symbol.endswith("-USD-PERP"): continue

                base = symbol.split('-')[0]

                # Парсинг
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
            # print(f"⚠️ Paradex Data Error: {e}")
            return {}

    # --- ІНШІ МЕТОДИ (АДАПТОВАНІ ПІД LOOP) ---

    def get_depth(self, symbol):
        if not self.paradex: return None
        sym = self._map_symbol(symbol)
        try:
            res = self._run_async(self.paradex.api_client.fetch_orderbook(market=sym))
            if res and 'bids' in res and 'asks' in res:
                best_bid = float(res['bids'][0][0])
                best_ask = float(res['asks'][0][0])
                return {'bid': best_bid, 'ask': best_ask}
            return None
        except:
            return None

    def get_account_balance(self):
        if not self.paradex: return {}
        try:
            res = self._run_async(self.paradex.api_client.fetch_balances())
            results = res.get('results', [])

            balances = {}
            total_usd = 0.0
            for item in results:
                token = item['token']
                size = float(item['size'])
                if size > 0:
                    balances[token] = {'available': size}
                    if token in ["USDC", "USD"]:
                        total_usd += size

            print(f"   💵 PARADEX БАЛАНС (USDC): ~{total_usd:.2f} $")
            print("=" * 30 + "\n")
            return balances
        except Exception as e:
            print(f"❌ Balance Error: {e}")
            return {}

    def get_positions(self):
        if not self.paradex: return []
        try:
            res = self._run_async(self.paradex.api_client.fetch_positions())
            results = res.get('results', [])

            cleaned = []
            for p in results:
                size = float(p['size'])
                if size == 0: continue

                # Логіка визначення сторони
                raw_side = p['side'].upper()
                if raw_side in ["LONG", "BUY"]:
                    side = "Long"
                elif raw_side in ["SHORT", "SELL"]:
                    side = "Short"
                else:
                    side = "Long" if size > 0 else "Short"

                cleaned.append({
                    'symbol': p['market'],
                    'side': side,
                    'netQuantity': str(abs(size)),
                    'entryPrice': p['average_entry_price'],
                    'liquidationPrice': p.get('liquidation_price', '0')
                })
            return cleaned
        except Exception as e:
            print(f"⚠️ Get Positions Error: {e}")
            return []

    # --- ТОРГІВЛЯ ---

    def place_limit_order(self, symbol, side, price, quantity):
        if not self.paradex: return None
        sym = self._map_symbol(symbol)
        p_side = OrderSide.Buy if side.lower() in ["buy", "long"] else OrderSide.Sell

        qty_dec = Decimal(str(quantity))
        price_dec = Decimal(str(price))
        client_id = f"bot_{int(time.time() * 1000)}"

        try:
            order = Order(
                market=sym,
                order_type=OrderType.Limit,
                order_side=p_side,
                size=qty_dec,
                limit_price=price_dec,
                client_id=client_id,
                instruction="GTC"
            )
            # Викликаємо через LOOP
            response = self._run_async(self.paradex.submit_order(order=order))
            return response.get("id") or response.get("order_id")
        except Exception as e:
            print(f"❌ Limit Error: {e}")
            return None

    def place_market_order(self, symbol, side, quantity):
        if not self.paradex: return None
        sym = self._map_symbol(symbol)
        p_side = OrderSide.Buy if side.lower() in ["buy", "long"] else OrderSide.Sell
        qty_dec = Decimal(str(quantity))
        client_id = f"bot_mkt_{int(time.time() * 1000)}"

        try:
            order = Order(
                market=sym,
                order_type=OrderType.Market,
                order_side=p_side,
                size=qty_dec,
                client_id=client_id
            )
            # Викликаємо через LOOP
            response = self._run_async(self.paradex.submit_order(order=order))
            return response.get("id") or response.get("order_id")
        except Exception as e:
            print(f"❌ Market Error: {e}")
            return None

    def cancel_order(self, symbol, order_id):
        if not self.paradex: return False
        try:
            self._run_async(self.paradex.api_client.cancel_order(order_id=str(order_id)))
            return True
        except:
            return False

    def get_open_orders(self, symbol=None):
        if not self.paradex: return []
        try:
            params = {}
            if symbol: params['market'] = self._map_symbol(symbol)
            res = self._run_async(self.paradex.api_client.fetch_orders(params=params))
            results = res.get('results', [])

            open_orders = [o for o in results if o['status'] in ['NEW', 'OPEN', 'TRIGGERED']]
            mapped = []
            for o in open_orders:
                mapped.append({
                    'id': o['id'],
                    'symbol': o['market'],
                    'price': o['price'],
                    'side': o['side'],
                    'quantity': o['size']
                })
            return mapped
        except:
            return []

    def close_position_market(self, symbol):
        positions = self.get_positions()
        pd_sym = self._map_symbol(symbol)
        target = next((p for p in positions if p['symbol'] == pd_sym), None)
        if not target:
            base = symbol.split('_')[0].split('-')[0]
            target = next((p for p in positions if base in p['symbol']), None)

        if not target: return False

        close_side = "Sell" if target['side'] == "Long" else "Buy"
        qty = target['netQuantity']
        return self.place_market_order(symbol, close_side, qty)

    def close_position_limit(self, symbol, price):
        positions = self.get_positions()
        pd_sym = self._map_symbol(symbol)
        target = next((p for p in positions if p['symbol'] == pd_sym), None)
        if not target:
            base = symbol.split('_')[0].split('-')[0]
            target = next((p for p in positions if base in p['symbol']), None)

        if not target: return None

        close_side = "Sell" if target['side'] == "Long" else "Buy"
        qty = target['netQuantity']
        return self.place_limit_order(symbol, close_side, price, qty)