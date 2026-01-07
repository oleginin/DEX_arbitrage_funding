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
        root_dir = Path(__file__).resolve().parent.parent
        env_path = root_dir / 'data' / '.env'
        load_dotenv(dotenv_path=env_path)

        l2_key = os.getenv("PARADEX_SUBKEY")
        l2_addr = os.getenv("PARADEX_ACCOUNT_ADDRESS")

        if not l2_key or not l2_addr:
            print("❌ Ключі PARADEX відсутні в .env!")
            self.paradex = None
            return

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        try:
            self.paradex = ParadexSubkey(
                env="prod",
                l2_private_key=l2_key,
                l2_address=l2_addr
            )
            # Init account
            self._run_async(self.paradex.init_account())
            print(f"✅ Paradex Initialized ({l2_addr[:6]}...)")

        except Exception as e:
            print(f"❌ Paradex Init Failed: {e}")
            self.paradex = None

    def _run_async(self, result):
        if asyncio.iscoroutine(result) or asyncio.isfuture(result):
            return self.loop.run_until_complete(result)
        return result

    def _map_symbol(self, symbol):
        # 1. If it already has PERP, assume it's correct
        if "PERP" in symbol: 
            return symbol
        
        # 2. Clean up common variations (ETH/USD, ETH_USDC)
        clean_sym = symbol.replace("/", "-").replace("_", "-").replace("USDC", "USD")
        
        # 3. Ensure base-quote structure
        if "-" not in clean_sym:
            # If input is just "ETH", assume "ETH-USD"
            clean_sym = f"{clean_sym}-USD"
            
        # 4. Append -PERP if missing
        return f"{clean_sym}-PERP"

    # --- INFO ---

    def get_market_data(self):
        if not self.paradex: return {}
        try:
            res = self._run_async(self.paradex.api_client.fetch_markets_summary(params={'market': 'ALL'}))
            results = res.get('results', []) if isinstance(res, dict) else res
            data = {}
            for item in results:
                symbol = item['symbol']
                if not symbol.endswith("-USD-PERP"): continue
                base = symbol.split('-')[0]
                bid = float(item.get('best_bid', item.get('bid', 0)) or 0)
                ask = float(item.get('best_ask', item.get('ask', 0)) or 0)
                # Paradex повертає funding_rate як ставку за 8 годин (як показує на сайті)
                # Тому просто множимо на 100 для отримання відсотків
                raw_fund = float(item.get('funding_rate_8h', item.get('funding_rate', 0)) or 0)
                # Якщо є funding_rate_8h - використовуємо його, інакше funding_rate (який теж за 8h)
                funding_pct = raw_fund * 100  # Конвертуємо в відсотки (вже за 8 годин)
                data[base] = {'bid': bid, 'ask': ask, 'funding_pct': funding_pct}
            return data
        except:
            return {}

    def get_depth(self, symbol):
        if not self.paradex: return None
        sym = self._map_symbol(symbol)
        try:
            res = self._run_async(self.paradex.api_client.fetch_orderbook(market=sym))
            if res and 'bids' in res and 'asks' in res:
                return {'bid': float(res['bids'][0][0]), 'ask': float(res['asks'][0][0])}
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
                size = float(item['size'])
                if size > 0:
                    balances[item['token']] = {'available': size}
                    if item['token'] in ["USDC", "USD"]: total_usd += size
            print(f"   💵 PARADEX БАЛАНС (USDC): ~{total_usd:.2f} $")
            return balances
        except Exception as e:
            print(f"❌ Balance Error: {e}")
            return {}

    def get_positions(self):
        if not self.paradex: return []
        try:
            res = self._run_async(self.paradex.api_client.fetch_positions())
            results = res.get('results', []) if isinstance(res, dict) else res
            cleaned = []
            for p in results:
                size = float(p['size'])
                if size == 0: continue
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

        try:
            order = Order(
                market=sym,
                order_type=OrderType.Limit,
                order_side=p_side,
                size=Decimal(str(quantity)),
                limit_price=Decimal(str(price)),
                client_id=f"bot_{int(time.time() * 1000)}",
                instruction="GTC"
            )
            # ВИПРАВЛЕНО: self.paradex.api_client.submit_order
            response = self._run_async(self.paradex.api_client.submit_order(order=order))
            return response.get("id") or response.get("order_id")
        except Exception as e:
            print(f"❌ Limit Error: {e}")
            return None

    def place_market_order(self, symbol, side, quantity):
        if not self.paradex: return None
        sym = self._map_symbol(symbol)
        p_side = OrderSide.Buy if side.lower() in ["buy", "long"] else OrderSide.Sell

        try:
            order = Order(
                market=sym,
                order_type=OrderType.Market,
                order_side=p_side,
                size=Decimal(str(quantity)),
                client_id=f"bot_mkt_{int(time.time() * 1000)}"
            )
            # ВИПРАВЛЕНО: self.paradex.api_client.submit_order
            response = self._run_async(self.paradex.api_client.submit_order(order=order))
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
            params = {'market': self._map_symbol(symbol)} if symbol else {}
            res = self._run_async(self.paradex.api_client.fetch_orders(params=params))
            results = res.get('results', []) if isinstance(res, dict) else res

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
        return self.place_market_order(symbol, close_side, target['netQuantity'])

    def close_position_limit(self, symbol, price):
        positions = self.get_positions()
        pd_sym = self._map_symbol(symbol)
        target = next((p for p in positions if p['symbol'] == pd_sym), None)
        if not target:
            base = symbol.split('_')[0].split('-')[0]
            target = next((p for p in positions if base in p['symbol']), None)

        if not target: return None
        close_side = "Sell" if target['side'] == "Long" else "Buy"
        return self.place_limit_order(symbol, close_side, price, target['netQuantity'])