import asyncio
import os
from decimal import Decimal
from pathlib import Path
from dotenv import load_dotenv
from uuid import UUID  # Потрібно для роботи з SDK

# --- COLORS ---
G, R, Y, B, X = "\033[92m", "\033[91m", "\033[93m", "\033[1m", "\033[0m"

try:
    from ethereal_trading import AsyncRESTClient
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

        # Caches
        self._products_cache = {}
        self._base_to_product = {}
        self._id_to_product = {}
        self._symbol_to_product = {}
        self._subaccount_id = None
        self._client = None
        self._client_lock = None

        try:
            self.loop = asyncio.get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        if not self.loop.is_running():
            self.loop.run_until_complete(self._initialize_cache())
        else:
            self.loop.create_task(self._initialize_cache())

    def _run_async(self, coro):
        if self.loop.is_running():
            return asyncio.create_task(coro)
        return self.loop.run_until_complete(coro)

    async def _initialize_cache(self):
        try:
            client = await self._get_auth_client()
            try:
                await self._ensure_products(client)
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
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()
        async with self._client_lock:
            if self._client is None:
                self._client = await self._get_auth_client()
            return self._client

    def _close_client(self):
        if self._client:
            if self.loop.is_running():
                self.loop.create_task(self._client.close())
            else:
                self.loop.run_until_complete(self._client.close())
            self._client = None

    # --- HELPERS ---
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

            # FIX: Ensure IDs are stored as strings to prevent int/str mismatches
            self._id_to_product = {str(p.id): ticker for ticker, p in products_map.items()}

            self._symbol_to_product = {}
            for ticker, product in products_map.items():
                self._symbol_to_product[ticker] = product
                normalized = ticker.replace("-", "").replace("_", "")
                if normalized not in self._symbol_to_product:
                    self._symbol_to_product[normalized] = product
        except Exception as e:
            print(f"{R}❌ Error initializing products: {e}{X}")
            pass

    def _find_product(self, symbol):
        product = self._symbol_to_product.get(symbol)
        if product: return product
        normalized = symbol.replace("-", "")
        product = self._symbol_to_product.get(normalized)
        if product: return product
        base_name = symbol.upper()
        product = self._base_to_product.get(base_name)
        if product: return product
        return None

    # --- CORE METHODS ---

    async def _get_depth_async(self, symbol):
        client = await self._get_reusable_client()
        try:
            if not self._symbol_to_product: await self._ensure_products(client)
            product = self._find_product(symbol)
            if not product: return None

            liquidity = await client.get_market_liquidity(product_id=product.id)
            if not liquidity or not liquidity.bids or not liquidity.asks: return None

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
                    name = (data.get('token_name') or data.get('asset_name') or "UNKNOWN")
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

            if not self._symbol_to_product: await self._ensure_products(client)
            product = self._find_product(symbol)

            final_ticker = symbol  # Default
            tick_size = 1

            if product:
                # FIX: Use str(product.id) to lookup the official ticker
                official_ticker = self._id_to_product.get(str(product.id))
                if official_ticker:
                    final_ticker = official_ticker

                tick_size = float(getattr(product, 'tick_size', 1))

            if price and order_type == "LIMIT":
                price_float = float(price)
                rounded_price = round(price_float / tick_size) * tick_size
                price_dec = Decimal(str(rounded_price))
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

    # --- НОВИЙ МЕТОД: REPLACE ORDER ---
    async def _replace_order_async(self, order_id, new_price, new_qty):
        """Використовує SDK для заміни ордера"""
        client = await self._get_reusable_client()
        try:
            # SDK вимагає UUID об'єкт
            uuid_id = UUID(str(order_id))

            # Конвертуємо параметри
            price_dec = float(new_price)  # SDK сам сконвертує в Decimal всередині або ми передамо float
            qty_dec = float(new_qty)

            # Викликаємо метод SDK
            # Повертає Tuple[SubmitOrderCreatedDto, bool]
            new_order_dto, success = await client.replace_order(
                order_id=uuid_id,
                price=price_dec,
                quantity=qty_dec
            )

            if success:
                print(f"   🔄 Replaced: {order_id} -> {new_order_dto.id}")
                return str(new_order_dto.id)
            else:
                print(f"   ❌ Replace returned success=False")
                return None
        except Exception as e:
            # print(f"Replace Error: {e}") # Часто буває, якщо ордер вже виконався
            return None

    async def _get_open_orders_async(self):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return []
                self._subaccount_id = subaccounts[0].id

            if not self._symbol_to_product: await self._ensure_products(client)

            orders = await client.list_orders(subaccount_id=self._subaccount_id)
            open_orders = []
            for o in orders:
                data = o.model_dump()
                raw_status = str(data.get('status')).upper()
                if 'CANCELED' in raw_status or 'FILLED' in raw_status or 'REJECTED' in raw_status:
                    continue

                symbol = data.get('ticker')
                if not symbol:
                    pid = data.get('product_id')
                    symbol = self._id_to_product.get(pid, "UNKNOWN")

                raw_side = data.get('side')
                side = "Buy" if str(raw_side) in ['0', 'Side.buy'] else "Sell"

                open_orders.append({
                    'id': str(data.get('id')),
                    'symbol': symbol,
                    'price': float(data.get('price', 0)),
                    'quantity': float(data.get('quantity', 0)),
                    'side': side,
                    'status': raw_status
                })
            return open_orders
        except:
            return []

    async def _cancel_order_async(self, order_id):
        client = await self._get_reusable_client()
        try:
            await client.cancel_orders(order_ids=[str(order_id)])
            return True
        except:
            return False

    async def _parse_position(self, p_data):
        try:
            size = float(p_data.get('size', 0))
            if size == 0: return None

            # 1. Символ
            pid = p_data.get('product_id')
            symbol = self._id_to_product.get(str(pid)) or self._id_to_product.get(UUID(str(pid)))
            if not symbol: symbol = "UNKNOWN"

            # 2. Ціни
            cost = float(p_data.get('cost', 0))  # Це номінал позиції (Entry Notional)
            entry_price = cost / size if size != 0 else 0

            # Ліквідація
            liq_raw = p_data.get('liquidation_price')
            liq_price = float(liq_raw) if liq_raw is not None else 0.0

            # 3. Розрахунок Плеча (Приблизний, на основі ліквідації)
            # Формула: Lev = Entry / (Entry - Liq) для Long
            leverage = 0.0
            if liq_price > 0 and entry_price > 0:
                diff = abs(entry_price - liq_price)
                if diff > 0:
                    leverage = entry_price / diff

            # Сторона
            raw_side = str(p_data.get('side')).lower()
            side = "Long" if ('0' in raw_side or 'buy' in raw_side) else "Short"

            return {
                'symbol': symbol,
                'side': side,
                'netQuantity': size,
                'entryPrice': entry_price,
                'liquidationPrice': liq_price,
                'notional': cost,  # <--- Додали вартість позиції в $
                'leverage': round(leverage, 2)  # <--- Додали розрахункове плече
            }
        except Exception as e:
            return None

    async def _get_positions_async(self):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return []
                self._subaccount_id = subaccounts[0].id
            if not self._id_to_product: await self._ensure_products(client)
            positions = await client.list_positions(subaccount_id=self._subaccount_id)
            clean_pos = []
            for p in positions:
                parsed = await self._parse_position(p.model_dump())
                if parsed: clean_pos.append(parsed)
            return clean_pos
        except:
            return []

    async def _get_specific_position_async(self, symbol):
        client = await self._get_reusable_client()
        try:
            if not self._subaccount_id:
                subaccounts = await client.subaccounts()
                if not subaccounts: return None
                self._subaccount_id = subaccounts[0].id
            if not self._symbol_to_product: await self._ensure_products(client)
            product = self._find_product(symbol)
            if not product: return None
            positions = await client.list_positions(subaccount_id=self._subaccount_id, product_ids=[product.id])
            if not positions: return None
            for p in positions:
                parsed = await self._parse_position(p.model_dump())
                if parsed: return parsed
            return None
        except:
            return None

    # --- WRAPPERS ---
    def get_account_balance(self):
        return self.loop.run_until_complete(self._get_balance_async())

    def place_limit_order(self, symbol, side, price, qty):
        res = self.loop.run_until_complete(self._place_order_async(symbol, side, qty, price, "LIMIT"))
        return str(getattr(res, "id", res)) if res else None

    def place_market_order(self, symbol, side, qty):
        res = self.loop.run_until_complete(self._place_order_async(symbol, side, qty, price=0, order_type="MARKET"))
        return str(getattr(res, "id", res)) if res else None

    def replace_limit_order(self, order_id, new_price, new_qty):
        return self.loop.run_until_complete(self._replace_order_async(order_id, new_price, new_qty))

    def get_positions(self):
        return self.loop.run_until_complete(self._get_positions_async())

    def cancel_order(self, symbol, order_id):
        return self.loop.run_until_complete(self._cancel_order_async(order_id))

    def get_open_orders(self, symbol=None):
        orders = self.loop.run_until_complete(self._get_open_orders_async())
        if symbol: return [o for o in orders if symbol in o.get('symbol', '')]
        return orders

    def get_depth(self, symbol):
        return self.loop.run_until_complete(self._get_depth_async(symbol))

    async def _get_data_async(self):
        """Паралельне отримання даних з обмеженням швидкості (Anti-429)"""
        client = await self._get_reusable_client()
        market_data = {}
        try:
            if not self._products_cache: await self._ensure_products(client)

            valid_products = []
            for ticker, product in self._products_cache.items():
                if "USD" in ticker:
                    valid_products.append(product)

            # --- ВИПРАВЛЕННЯ: SEMAPHORE ---
            # Дозволяємо лише 5 одночасних запитів.
            # Інші будуть чекати в черзі. Це врятує від бану IP.
            sem = asyncio.Semaphore(5)

            async def fetch_safe(product):
                async with sem:
                    try:
                        # Отримуємо ліквідність
                        liq = await client.get_market_liquidity(product_id=product.id)
                        # Робимо мікро-паузу, щоб не спамити API занадто часто
                        await asyncio.sleep(0.05)
                        return product, liq
                    except:
                        return product, None

            # Створюємо завдання з "гальмами"
            tasks = [fetch_safe(p) for p in valid_products]

            # Виконуємо
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Парсимо результати
            for res in results:
                if isinstance(res, Exception) or not res or not res[1]: continue

                product, liquidity = res
                if not liquidity.bids or not liquidity.asks: continue

                market_data[product.base_token_name] = {
                    'bid': float(liquidity.bids[0][0]),
                    'ask': float(liquidity.asks[0][0]),
                    'funding_pct': float(product.funding_rate1h or 0) * 100
                }
        except Exception as e:
            print(f"Fetch error: {e}")
        return market_data

    def get_position(self, symbol):
        return self.loop.run_until_complete(self._get_specific_position_async(symbol))

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
                    await self._ensure_products(client);
                    return list(self._base_to_product.keys())
                finally:
                    await client.close()

            return asyncio.run(_inner())
        except:
            return []

    def get_funding_rate(self, symbol):
        return 0.0

    async def close(self):
        """Gracefully close the client session"""
        if self._client:
            await self._client.close()
            self._client = None

    # --- MARKET & CLOSE METHODS ---

    async def _open_market_async(self, symbol, side, qty):
        """Відкриває позицію по маркету (миттєве виконання)"""
        print(f"{B}⚡ MARKET EXECUTION: {side} {qty} {symbol}{X}")
        # Викликаємо стандартний метод, але з типом MARKET і ціною 0
        return await self._place_order_async(symbol, side, qty, price=0, order_type="MARKET")

    async def _close_position_async(self, symbol):
        """Повністю закриває існуючу позицію по маркету"""
        # 1. Отримуємо актуальну позицію
        pos = await self._get_specific_position_async(symbol)

        if not pos or float(pos['netQuantity']) == 0:
            print(f"{Y}⚠️ No active position on {symbol} to close.{X}")
            return False

        # 2. Визначаємо параметри для закриття
        qty = float(pos['netQuantity'])
        current_side = pos['side']  # "Long" або "Short"

        # Щоб закрити Long, треба Sell. Щоб закрити Short, треба Buy.
        close_side = "Sell" if current_side == "Long" else "Buy"

        print(f"{R}🚨 CLOSING POSITION: {current_side} {qty} {symbol} -> {close_side} (Market){X}")

        # 3. Відправляємо ордер
        res = await self._place_order_async(symbol, close_side, qty, price=0, order_type="MARKET")

        if res:
            print(f"{G}✅ Position Closed.{X}")
            return True
        else:
            print(f"{R}❌ Close Failed.{X}")
            return False

    # Додати в кінець класу:
    def open_market(self, s, side, q):
        res = self.loop.run_until_complete(self._open_market_async(s, side, q))
        return str(res.id) if res else None

    def close_position(self, s):
        return self.loop.run_until_complete(self._close_position_async(s))