import asyncio
import json
import re
from csv import DictWriter

import aiocsv
import aiofiles
from aiohttp import ClientSession
from bs4 import BeautifulSoup


class ParserDior:

    def __init__(self, url, bar=None, create_csv=True):
        self.url = url[0]
        self.target = url[1]
        self.product_list = []
        self.detail_url = "https://api-fashion.dior.com/graph?GetProductStocks="
        self.dior_url = 'https://www.dior.com'
        self.rate_sem = asyncio.BoundedSemaphore(85)
        self.main_body_bags = {
            "requests": [
                {
                    "query": "",
                    "indexName": "merch_prod_live_ru_ru",
                    "params": 'hitsPerPage=1000&restrictSearchableAttributes=["ean","sku"]&attributesToRetrieve=["color","style_ref","title", "products", "subtitle", "size", "products"]&filters=namespaces:women-allthebags OR namespaces:women-allthebags-2 OR namespaces:women-allthebags-3 OR namespaces:women-allthebags-4 AND type:"PRODUCT"&facets=["color.group",]'
                }
            ]
        }
        self.main_body_belts = {
            "requests": [
                {
                    "query": "",
                    "indexName": "merch_prod_live_ru_ru",
                    "params": 'hitsPerPage=1000&restrictSearchableAttributes=["ean","sku"]&attributesToRetrieve=["color","style_ref","title", "products", "subtitle", "size", "products"]&filters=namespaces:women-accessories-belts AND type:"PRODUCT"'
                }
            ]
        }
        self.second_body = {"operationName": "GetProductStocks",
                            "variables": {"id": ""},
                            "query": "query GetProductStocks($id: String!) {\n  product: getProduct(id: $id) {"
                                     "code,"
                                     "sizeAndFit,"
                                     "url,"
                                     "}}"
                            }
        self.main_headers = {
            "x-algolia-api-key": "64e489d5d73ec5bbc8ef0d7713096fba",
            "x-algolia-application-id": "KPGNQ6FJI9",
            "Host": "kpgnq6fji9-dsn.algolia.net",
            "Origin": "https://www.dior.com",
            "Referer": "https://www.dior.com/",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0"
        }
        self.second_headers = {
            "Host": "api-fashion.dior.com",
            "apollographql-client-name": "Newlook Couture Catalog K8S",
            "apollographql-client-version": "5.247.1-8f13cb5e.hotfix",
            "x-dior-universe": "couture",
            "x-dior-locale": "ru_ru"
        }

        self.fieldnames = ['article', 'title', 'subtitle', 'size_and_fit', 'colours', 'photos']

        self.bar = bar
        if create_csv:
            self.init_csv()

    def init_csv(self):
        with open(f'./parser/dior/csv/{self.target}.csv',
                  'w') as file:
            writer = DictWriter(file, fieldnames=self.fieldnames, delimiter=';')
            writer.writeheader()

    async def get_photos(self, url, code):
        photos = []
        async with ClientSession() as session:
            async with session.get(self.dior_url + url) as response:
                extra_soup = BeautifulSoup(await response.text('utf-8'), "lxml")
                photos_raw = extra_soup.find_all('img', alt=re.compile('aria_openGallery'))
                if not photos_raw:
                    async with session.get(self.dior_url + f"/{code}") as response:
                        extra_soup = BeautifulSoup(await response.text('utf-8'), "lxml")
                        photos_raw = extra_soup.find_all('img', alt=re.compile('aria_openGallery'))
                photos.append([link.attrs['src'] for link in photos_raw])
        await asyncio.sleep(0.5)
        return photos[0]

    async def create_csv(self, article, title, subtitle, size_and_fit, colours, photos_links):
        async with aiofiles.open(f'./parser/dior/csv/{self.target}.csv',
                                 'a') as file:
            writer = aiocsv.AsyncDictWriter(file, fieldnames=self.fieldnames, delimiter=';')
            await writer.writerow({
                'article': article,
                'title': title,
                'subtitle': subtitle,
                'size_and_fit': size_and_fit,
                'colours': colours,
                'photos': photos_links,
            })
        self.bar()
        await asyncio.sleep(0.1)

    async def get_links(self):
        async with ClientSession(headers=self.main_headers) as session:
            async with session.post(
                    self.url, json=self.main_body_bags if self.target == 'bags' else self.main_body_belts) as response:
                main_json = json.loads(await response.text())
        self.product_list = main_json['results'][0]['hits']
        return self.product_list

    async def delay_wrapper(self, task):
        await self.rate_sem.acquire()
        return await task

    async def releaser(self):
        while True:
            await asyncio.sleep(0.05)
            try:
                self.rate_sem.release()
            except ValueError:
                pass

    async def main(self):
        await self.get_links()
        rt = asyncio.create_task(self.releaser())
        await asyncio.gather(
            *[self.delay_wrapper(self.collect_dior(product)) for product in self.product_list])
        rt.cancel()

    async def collect_dior(self, product):
        try:
            article = f"{product['style_ref']}_{product['color']['code']}"
            self.second_body['variables']['id'] = article
            async with ClientSession(headers=self.second_headers) as session:
                async with session.post(self.detail_url, json=self.second_body) as response:
                    second_json = json.loads(await response.text())
                    product.update(second_json['data']['product'])
            await asyncio.sleep(0.1)
            try:
                title = product['title']
            except BaseException as e:
                title = '--'
            try:
                subtitle = product['subtitle']
            except BaseException as e:
                subtitle = '--'
            try:
                size_and_fit = product['sizeAndFit'].replace('<br />', '').replace(
                    'Подробную информацию см. в размерной сетке.', '').replace('\n', ' ')
            except BaseException as e:
                size_and_fit = '--'
            try:
                colours = product['color']['group']
            except BaseException as e:
                colours = '--'
            try:
                photos = await self.get_photos(product['url'], product['code'])
            except BaseException as e:
                print(e)
            if not photos:
                raise BaseException
            await self.create_csv(article, title, subtitle, size_and_fit, colours, photos)
        except BaseException as e:
            print(f"Критическая ошибка -- пропуск ссылки -- сайт {self.url}")
            return
