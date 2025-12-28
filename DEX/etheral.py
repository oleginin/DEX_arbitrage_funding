import asyncio
from ethereal import AsyncRESTClient

async def list_products():
    client = await AsyncRESTClient.create({
        "base_url": "https://api.ethereal.trade",
    })

    # Get all available products
    products = await client.list_products()

    for product in products:
        print(product.model_dump())

    await client.close()

asyncio.run(list_products())