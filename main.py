import asyncio
import csv
import time

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientError


base_url = 'https://api.detmir.ru/v2/products'


async def get_product_count(session: ClientSession) -> int:
    params = {'filter': f'categories[].alias:lego;promo:false', 'meta': '*', 'limit': 1,
              'offset': 0}
    async with session.get(base_url, params=params) as response:
        meta_json = await response.json()
        return meta_json['meta']['length']


async def write_products(writer, session: ClientSession, offset: int):
    params = {'filter': f'categories[].alias:lego;promo:false', 'limit': 100, 'offset': offset}
    connection_attempts = 0
    while connection_attempts < 3:
        try:
            async with session.get(base_url, params=params, timeout=5) as response:
                products_json = await response.json()
                for item in products_json:
                    stock = item['available']['offline']['region_iso_codes']
                    if 'RU-MOW' in stock or 'RU-SPE' in stock:
                        product_id = item['id']
                        title = item['title']
                        if item['old_price']:
                            price = item['old_price']['price']
                            promo_price = item['price']['price']
                        else:
                            price = item['price']['price']
                            promo_price = None
                        product_url = item['link']['web_url']
                        writer.writerow([product_id, title, price, promo_price, product_url])
            break
        except (ClientError, asyncio.exceptions.TimeoutError):
            connection_attempts += 1
    if connection_attempts == 3:
        print(f"Failed to connect host. Elements from {offset} to {offset + 100} doesn't write")


async def main():
    async with ClientSession() as session:
        tasks = []
        try:
            count = await get_product_count(session)
        except (ClientError, asyncio.exceptions.TimeoutError):
            print('Failed to connect host.')
        else:
            print(f'Connections have been successfully created. {count} products were found.')
            with open('output.csv', 'w', newline='') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(['id', 'title', 'price', 'promo_price', 'url'])
                for offset in range(0, count, 100):
                    tasks.append(asyncio.ensure_future(write_products(writer, session, offset)))
                await asyncio.gather(*tasks)


start = time.time()
asyncio.get_event_loop().run_until_complete(main())
total_time = round(time.time() - start, 3)
print(f"The program's working time is {total_time} seconds")
