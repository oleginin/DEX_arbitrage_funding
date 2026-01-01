import asyncio
from ethereal import AsyncRESTClient


class EtherealEngine:
    def __init__(self):
        self.base_url = "https://api.ethereal.trade"

    async def _get_data_async(self):
        """
        Внутрішній асинхронний метод.
        """
        client = await AsyncRESTClient.create({"base_url": self.base_url})
        market_data = {}
        try:
            products_map = await client.products_by_ticker()
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