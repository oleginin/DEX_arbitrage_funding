import asyncio
from ethereal import AsyncRESTClient


class EtherealEngine:
    def __init__(self):
        self.base_url = "https://api.ethereal.trade"
        # Cache product metadata to avoid refetching on every call
        self._products_cache = {}
        self._base_to_product = {}

    async def _ensure_products(self, client):
        if self._base_to_product:
            return
        try:
            products_map = await client.products_by_ticker()
            self._products_cache = products_map
            self._base_to_product = {
                product.base_token_name.upper(): product
                for ticker, product in products_map.items()
                if "USD" in ticker
            }
        except Exception as e:
            print(f"⚠️ Ethereal Products Error: {e}")
            self._products_cache = {}
            self._base_to_product = {}

    async def _get_depth_async(self, base_symbol):
        client = await AsyncRESTClient.create({"base_url": self.base_url})
        try:
            await self._ensure_products(client)
            product = self._base_to_product.get(base_symbol.upper())
            if not product:
                return None

            liquidity = await client.get_market_liquidity(product_id=product.id)
            if not liquidity or not liquidity.bids or not liquidity.asks:
                return None

            best_bid = float(liquidity.bids[0][0])
            best_ask = float(liquidity.asks[0][0])
            return {"bid": best_bid, "ask": best_ask}
        except Exception as e:
            print(f"⚠️ Ethereal Depth Error ({base_symbol}): {e}")
            return None
        finally:
            await client.close()

    async def _get_funding_async(self, base_symbol):
        client = await AsyncRESTClient.create({"base_url": self.base_url})
        try:
            await self._ensure_products(client)
            product = self._base_to_product.get(base_symbol.upper())
            if not product:
                return 0.0
            raw_fund = float(product.funding_rate1h or 0)
            return raw_fund  # already 1h rate; caller can scale if needed
        except Exception as e:
            print(f"⚠️ Ethereal Funding Error ({base_symbol}): {e}")
            return 0.0
        finally:
            await client.close()

    def get_perp_symbols(self):
        """
        Returns list of base symbols for USD perpetuals.
        """
        async def _inner():
            client = await AsyncRESTClient.create({"base_url": self.base_url})
            try:
                await self._ensure_products(client)
                return list(self._base_to_product.keys())
            finally:
                await client.close()

        try:
            return asyncio.run(_inner())
        except Exception as e:
            print(f"⚠️ Ethereal Symbols Error: {e}")
            return []

    def get_depth(self, symbol):
        try:
            return asyncio.run(self._get_depth_async(symbol))
        except Exception as e:
            print(f"⚠️ Ethereal Depth Wrapper Error ({symbol}): {e}")
            return None

    def get_funding_rate(self, symbol):
        """
        Returns 1h funding rate (decimal). Matches Backpack's get_funding_rate signature.
        """
        try:
            return asyncio.run(self._get_funding_async(symbol))
        except Exception as e:
            print(f"⚠️ Ethereal Funding Wrapper Error ({symbol}): {e}")
            return 0.0

    async def _get_data_async(self):
        """
        Внутрішній асинхронний метод.
        """
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
                if isinstance(liquidity, Exception) or not liquidity.bids or not liquidity.asks:
                    continue

                best_bid = float(liquidity.bids[0][0])
                best_ask = float(liquidity.asks[0][0])

                # Ethereal raw funding is decimal 1h rate
                raw_fund = float(product.funding_rate1h or 0)

                market_data[product.base_token_name] = {
                    'bid': best_bid,
                    'ask': best_ask,
                    'funding_pct': raw_fund * 100
                }
        except Exception as e:
            print(f"⚠️ Ethereal Internal Error: {e}")
        finally:
            await client.close()
        return market_data

    def get_market_data(self):
        """
        СИНХРОННИЙ МЕТОД (Wrapper).
        Викликається з monitor_spread_fund.py.
        Запускає асинхронний код і чекає на результат.
        """
        try:
            # asyncio.run запускає цикл подій, виконує корутину і повертає результат (словник)
            return asyncio.run(self._get_data_async())
        except Exception as e:
            print(f"⚠️ Ethereal Wrapper Error: {e}")
            return {}