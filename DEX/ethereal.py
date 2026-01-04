import asyncio
import os
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv

# --- КОЛЬОРИ ---
G, R, Y, B, X = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[0m"

# Імпорти Ethereal
try:
    from ethereal import AsyncRESTClient
except ImportError:
    print(f"{R}❌ Ethereal SDK not found. Install via: pip install ethereal-sdk{X}")


class EtherealEngine:
    def __init__(self):
        root_dir = Path(__file__).resolve().parent.parent
        env_path = root_dir / 'data' / '.env'
        load_dotenv(dotenv_path=env_path)

        self.base_url = "https://api.ethereal.trade"
        self.rpc_url = "https://rpc.ethereal.trade"

        self.private_key = os.getenv("ETHEREAL_PRIVATE_KEY")

        self._products_cache = {}
        self._base_to_product = {}
        self._id_to_product = {}
        self._symbol_to_product = {}  # Fast lookup: symbol -> product
        self._subaccount_id = None  # Cache subaccount ID
        self._client = None  # Reusable client connection
        self._client_lock = None  # Will be initialized when needed

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        # Pre-load products and subaccount on init for faster subsequent calls
        try:
            self._run_async(self._initialize_cache())
        except:
            pass  # Fail silently, will load on first use

    def _run_async(self, coro):
        return self.loop.run_until_complete(coro)

    async def _initialize_cache(self):
        """Pre-load products and subaccount ID for faster operations"""
        try:
            client = await self._get_auth_client()
            try:
                # Load products
                await self._ensure_products(client)
                # Cache subaccount ID
                subaccounts = await client.subaccounts()
                if subaccounts:
                    self._subaccount_id = subaccounts[0].id
            finally:
                await client.close()
        except:
            pass

    async def _get_auth_client(self):
        if not self.private_key:
            raise ValueError("❌ ETHEREAL_PRIVATE_KEY not found in .env")
        return await AsyncRESTClient.create({
            "base_url": self.base_url,
            "chain_config": {
                "rpc_url": self.rpc_url,
                "private_key": self.private_key
            }
        })

    async def _get_reusable_client(self):
        """Get or create a reusable client connection"""
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()
        async with self._client_lock:
            if self._client is None:
                self._client = await self._get_auth_client()
            return self._client

    def _close_client(self):
        """Close the reusable client (call when done with batch operations)"""
        if self._client:
            self._run_async(self._client.close())
            self._client = None

    # --- ДОПОМІЖНІ ---
    async def _ensure_products(self, client):
        if self._base_to_product: return
        try:
            products_map = await client.products_by_ticker()
            self._products_cache = products_map
            self._base_to_product = {
                product.base_token_name.upper(): product
                for ticker, product in products_map.items()
                if "USD" in ticker
            }
            self._id_to_product = {p.id: ticker for ticker, p in products_map.items()}
            # Fast symbol lookup cache
            self._symbol_to_product = {}
            for ticker, product in products_map.items():
                # Store exact match
                self._symbol_to_product[ticker] = product
                # Store normalized (no dashes) for flexible matching
                normalized = ticker.replace("-", "")
                if normalized not in self._symbol_to_product:
                    self._symbol_to_product[normalized] = product
        except Exception as e:
            pass

    def _find_product(self, symbol):
        """Fast product lookup using cache"""
        # Try exact match first (e.g., "BTC-USD")
        product = self._symbol_to_product.get(symbol)
        if product:
            return product
        
        # Try normalized match (e.g., "BTCUSD" -> "BTC-USD")
        normalized = symbol.replace("-", "")
        product = self._symbol_to_product.get(normalized)
        if product:
            return product
        
        # Try base token name match (e.g., "BTC" -> product)
        # This handles cases where get_perp_symbols() returns just "BTC"
        base_name = symbol.upper()
        product = self._base_to_product.get(base_name)
        if product:
            return product
        
        # Fallback: iterate through products_cache like original code
        # This handles edge cases
        for k, p in self._products_cache.items():
            if k.replace("-", "") == symbol.replace("-", ""):
                return p
        return None

    # --- ОСНОВНІ МЕТОДИ ---

    async def _get_depth_async(self, symbol):
        client = await self._get_reusable_client()
        try:
            # Ensure products are loaded
            if not self._symbol_to_product:
                await self._ensure_products(client)

            product = self._find_product(symbol)
            if not product:
                return None

            liquidity = await client.get_market_liquidity(product_id=product.id)
            if not liquidity or not liquidity.bids or not liquidity.asks:
                return None

            return {
                'bid': float(liquidity.bids[0][0]),
                'ask': float(liquidity.asks[0][0])
            }
        except:
            return None

    async def _get_balance_async(self):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return {}
                self._subaccount_id = subaccounts[0].id
            balances = await client.get_subaccount_balances(subaccount_id=self._subaccount_id)

            result = {}
            for b in balances:
                try:
                    data = b.model_dump()
                    name = (data.get('token_name') or data.get('asset_name') or data.get('asset') or "UNKNOWN")
                    amount = float(data.get('amount', 0))
                    result[str(name)] = amount
                except:
                    pass
            return result
        except:
            return {}

    async def _place_order_async(self, symbol, side, qty, price=None, order_type="LIMIT"):
        client = await self._get_reusable_client()
        try:
            side_int = 0 if side.upper() in ["BUY", "LONG"] else 1
            qty_dec = Decimal(str(qty))

            # Auto-map ticker using cache - use same logic as _find_product
            if not self._symbol_to_product:
                await self._ensure_products(client)
            
            # Find the product first (handles "BTC" -> "BTC-USD" mapping)
            product = self._find_product(symbol)
            if product:
                # Get the ticker from _id_to_product mapping
                final_ticker = self._id_to_product.get(product.id, symbol)
                
                # Get tick size from product (default to 1 if not available)
                tick_size = getattr(product, 'tick_size', None) or getattr(product, 'tickSize', None) or 1
                tick_size = float(tick_size)
            else:
                # Fallback: try original symbol or normalized match
                final_ticker = symbol
                tick_size = 1  # Default tick size
                if symbol not in self._products_cache:
                    for ticker in self._products_cache.keys():
                        if ticker.replace("-", "") == symbol.replace("-", ""):
                            final_ticker = ticker
                            break

            # Round price to nearest tick size for limit orders
            if price and order_type == "LIMIT":
                price_float = float(price)
                rounded_price = round(price_float / tick_size) * tick_size
                price_dec = Decimal(str(rounded_price))
                if rounded_price != price_float:
                    print(f"   {Y}⚠️  Price rounded: {price_float} -> {rounded_price} (tickSize: {tick_size}){X}")
            else:
                price_dec = Decimal(str(price)) if price else Decimal("0")

            print(f"   🚀 Sending Order: {side} {qty} {final_ticker} @ {price_dec or 'MKT'}")

            order = await client.create_order(
                ticker=final_ticker,
                side=side_int,
                quantity=qty_dec,
                price=price_dec,
                order_type=order_type,
            )
            return order
        except Exception as e:
            print(f"{R}❌ Order Failed: {e}{X}")
            return None

    async def _get_open_orders_async(self):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return []
                self._subaccount_id = subaccounts[0].id

            if not self._symbol_to_product:
                await self._ensure_products(client)

            # Отримуємо всі ордери
            orders = await client.list_orders(subaccount_id=self._subaccount_id)

            open_orders = []
            for o in orders:
                data = o.model_dump()

                # --- ФІЛЬТРАЦІЯ СТАТУСУ ---
                raw_status = str(data.get('status')).upper()
                if 'CANCELED' in raw_status or 'FILLED' in raw_status or 'REJECTED' in raw_status:
                    continue

                # SYMBOL
                symbol = data.get('ticker')
                if not symbol:
                    pid = data.get('product_id')
                    symbol = self._id_to_product.get(pid, "UNKNOWN")

                # SIDE
                raw_side = data.get('side')
                side = "Sell"
                if str(raw_side).upper() in ['0', 'BUY', 'LONG']:
                    side = "Buy"

                open_orders.append({
                    'id': str(data.get('id')),
                    'symbol': symbol,
                    'price': float(data.get('price', 0)),
                    'quantity': float(data.get('quantity', 0)),
                    'side': side,
                    'status': raw_status
                })
            return open_orders
        except Exception as e:
            return []

    async def _cancel_order_async(self, order_id):
        client = await self._get_reusable_client()
        try:
            await client.cancel_orders(order_ids=[str(order_id)])
            return True
        except Exception as e:
            print(f"{R}❌ Cancel Error: {e}{X}")
            return False

    # --- ОТРИМАННЯ ВСІХ ПОЗИЦІЙ ---
    async def _get_positions_async(self):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return []
                self._subaccount_id = subaccounts[0].id

            positions = await client.list_positions(subaccount_id=self._subaccount_id)
            clean_pos = []

            for p in positions:
                data = p.model_dump()
                size = float(data.get('quantity', 0))
                if size == 0: continue

                raw_side = data.get('side')
                side = "Long" if str(raw_side) == '0' else "Short"

                clean_pos.append({
                    'symbol': data.get('ticker') or data.get('product_name') or "UNKNOWN",
                    'side': side,
                    'netQuantity': size,
                    'entryPrice': float(data.get('entry_price', 0)),
                    'liquidationPrice': float(data.get('liquidation_price', 0))
                })
            return clean_pos
        except Exception as e:
            return []

    # --- НОВИЙ МЕТОД: ПЕРЕВІРКА ПОЗИЦІЇ ПО СИМВОЛУ ---
    async def _get_specific_position_async(self, symbol):
        """Повертає позицію для конкретного символу або None"""
        client = await self._get_reusable_client()
        try:
            # Use cached subaccount ID
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return None
                self._subaccount_id = subaccounts[0].id

            # Ensure products are loaded
            if not self._symbol_to_product:
                await self._ensure_products(client)

            # Fast product lookup
            target_product = self._find_product(symbol)
            if not target_product:
                return None

            # Фільтруємо позиції через API (ефективніше)
            positions = await client.list_positions(
                subaccount_id=self._subaccount_id,
                product_ids=[target_product.id]
            )

            if not positions:
                return None

            # Парсимо першу знайдену
            data = positions[0].model_dump()
            size = float(data.get('quantity', 0))
            if size == 0: return None  # Позиція є, але розмір 0 (закрита)

            raw_side = data.get('side')
            side = "Long" if str(raw_side) == '0' else "Short"

            return {
                'symbol': symbol,
                'side': side,
                'netQuantity': size,
                'entryPrice': float(data.get('entry_price', 0)),
                'liquidationPrice': float(data.get('liquidation_price', 0))
            }

        except Exception as e:
            # print(f"Error checking position: {e}")
            return None

    # --- WRAPPERS ---
    def get_account_balance(self):
        try:
            return self._run_async(self._get_balance_async())
        except:
            return {}

    def place_limit_order(self, symbol, side, price, qty):
        try:
            res = self._run_async(self._place_order_async(symbol, side, qty, price, "LIMIT"))
            return str(getattr(res, "id", res)) if res else None
        except:
            return None

    def place_market_order(self, symbol, side, qty):
        try:
            res = self._run_async(self._place_order_async(symbol, side, qty, price=0, order_type="MARKET"))
            return str(getattr(res, "id", res)) if res else None
        except:
            return None

    def get_positions(self):
        try:
            return self._run_async(self._get_positions_async())
        except:
            return []

    def cancel_order(self, symbol, order_id):
        try:
            return self._run_async(self._cancel_order_async(order_id))
        except:
            return False

    def get_open_orders(self, symbol=None):
        try:
            orders = self._run_async(self._get_open_orders_async())
            if symbol: return [o for o in orders if symbol in o.get('symbol', '')]
            return orders
        except:
            return []

    def get_depth(self, symbol):
        try:
            return self._run_async(self._get_depth_async(symbol))
        except:
            return None

    # Виклик нового методу
    def get_position(self, symbol):
        try:
            return self._run_async(self._get_specific_position_async(symbol))
        except:
            return None

    # --- MONITOR ---
    async def _get_data_async(self):
        client = await AsyncRESTClient.create({"base_url": self.base_url})
        market_data = {}
        try:
            await self._ensure_products(client)
            products_map = self._products_cache
            tasks = []
            valid_products = []
            for ticker, product in products_map.items():
                if "USD" in ticker:
                    tasks.append(client.get_market_liquidity(product_id=product.id))
                    valid_products.append(product)
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for product, liquidity in zip(valid_products, results):
                if isinstance(liquidity, Exception) or not liquidity.bids or not liquidity.asks: continue
                best_bid = float(liquidity.bids[0][0])
                best_ask = float(liquidity.asks[0][0])
                raw_fund = float(product.funding_rate1h or 0)
                market_data[product.base_token_name] = {
                    'bid': best_bid, 'ask': best_ask, 'funding_pct': raw_fund * 100
                }
        except:
            pass
        finally:
            await client.close()
        return market_data

    def get_market_data(self):
        try:
            return asyncio.run(self._get_data_async())
        except:
            return {}

    def get_perp_symbols(self):
        try:
            async def _inner():
                client = await AsyncRESTClient.create({"base_url": self.base_url})
                try:
                    await self._ensure_products(client); return list(self._base_to_product.keys())
                finally:
                    await client.close()

            return asyncio.run(_inner())
        except:
            return []

    def get_funding_rate(self, symbol):
        return 0.0