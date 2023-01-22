import asyncio
import re
from csv import DictWriter

import aiocsv
import aiofiles
from aiohttp import ClientSession, TCPConnector
from bs4 import BeautifulSoup, Tag, NavigableString


class ParserZilli:

    def __init__(self, url: str, bar, create_csv=True):
        self.task = None
        self.rate_sem = asyncio.BoundedSemaphore(30)
        self.product_list_links = []
        self.url = url
        self.more_detail_raw = []
        self.bar = bar
        self.fieldnames = ['article', 'title', 'subtitle', 'more_details', 'materials', 'colours', 'photos']

        if create_csv:
            self.init_csv()

    def init_csv(self):
        with open(f'./parser/zilli/csv/{self.url.split("en/")[1]}.csv',
                  'w') as file:
            writer = DictWriter(file, fieldnames=self.fieldnames, delimiter=';')
            writer.writeheader()

    async def translator(self, text):
        async with ClientSession() as session:
            url = f"https://translate.google.com/m?sl=en&tl=ru&hl=en&q={text}"
            async with session.get(url) as response:
                soup = BeautifulSoup(await response.text(), 'lxml')
                result_container = soup.find("div", {"class": "result-container"})
                if result_container:
                    translated_text = result_container.text
                else:
                    print('Слишком много запросов. Мы заблокированы(')
                    translated_text = text
        await asyncio.sleep(0.5)
        return translated_text

    async def create_csv(self, article, title, subtitle, more_detail, materials, colours, photos):
        async with aiofiles.open(
                f'./parser/zilli/csv/{self.url.split("en/")[1]}.csv',
                mode='a', encoding='utf-8') as file:
            writer = aiocsv.AsyncDictWriter(file, fieldnames=self.fieldnames, delimiter=';')
            await writer.writerow({
                'article': article,
                'title': title,
                'subtitle': subtitle,
                'more_details': more_detail,
                'materials': materials,
                'colours': colours,
                'photos': photos,
            })
        self.bar()
        await asyncio.sleep(0.5)

    async def get_title(self, soup, link):
        try:
            title = soup.find('h1', class_='main-title').text
            title = await self.translator(title)
        except BaseException as e:
            title = "--"
        await asyncio.sleep(0.5)
        return title

    async def get_subtitle(self, soup, link):
        try:
            subtitle = soup.find('div', id='product-description-short')
            subtitle_list = [item for item in subtitle.children if not isinstance(item, NavigableString)]
            if subtitle_list:
                subtitle = await self.translator(subtitle_list[0].text)
            else:
                subtitle = '--'
        except BaseException as e:
            subtitle = '--'
        await asyncio.sleep(0.5)
        return subtitle

    async def get_more_detail(self, soup, link):
        try:
            self.more_detail_raw = soup.select_one('.product_infos_tabs > li:nth-child(1) > div:nth-child(2) > '
                                                   'ul:nth-child(1)')
            if self.more_detail_raw:
                more_detail = ",,,".join(
                    [li_text.text for li_text in self.more_detail_raw.contents[:-2] if not isinstance(li_text,
                                                                                                      NavigableString)])
            else:
                more_detail = soup.select_one('.product_infos_tabs > li:nth-child(1) > div:nth-child(2) > '
                                              'ul:nth-child(1)').text
            more_detail = await self.translator(more_detail)
        except BaseException as e:
            more_detail = "--"
        await asyncio.sleep(0.5)
        return more_detail

    async def get_materials(self, soup, link):
        try:
            materials_raw = soup.select_one('.product_infos_tabs > li:nth-child(2) > div:nth-child(2) > '
                                            'ul:nth-child(1)')
            if materials_raw:
                materials = ",,,".join(
                    [li_text.text for li_text in materials_raw.contents if
                     not isinstance(li_text, NavigableString)])
            else:
                materials = soup.select_one('.product_infos_tabs > li:nth-child(2) > div:nth-child(2) > '
                                            'p:nth-child(1)').text
            materials = await self.translator(materials)
        except BaseException as e:
            materials = '--'
        await asyncio.sleep(0.5)
        return materials

    async def get_article(self, soup, link):
        try:
            article = soup.find('li', string=[re.compile(r"Ref. *"), re.compile(r"Réf. *"), re.compile(
                r"[A-Z0-9]{3,}-[A-Z0-9]{3,}-[A-Z0-9]{3,}/[A-Z0-9]{3,} [A-Z0-9]*")]).text
        except BaseException as e:
            if isinstance(self.more_detail_raw[-2], Tag) and len(self.more_detail_raw) > 2:
                article = self.more_detail_raw[-2].text
            elif isinstance(self.more_detail_raw[-2], Tag) and len(self.more_detail_raw) <= 2:
                article = re.search(r"[A-Z0-9]{3,}-[A-Z0-9]{3,}-[A-Z0-9]{3,}/[A-Z0-9]{3,} [A-Z0-9]*",
                                    self.more_detail_raw[-2].text).group(0)
            else:
                article = self.more_detail_raw[-1].text
        await asyncio.sleep(0.5)
        return article

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
            *[self.delay_wrapper(self.collect_zilli(link)) for link in self.product_list_links])
        rt.cancel()

    async def get_links(self):
        async with ClientSession(connector=TCPConnector(verify_ssl=False)) as session:
            async with session.get(self.url) as main_response:
                main_soup = BeautifulSoup(await main_response.text(), "lxml")
                self.product_list_links = main_soup.find_all('a', class_="product-thumbnail")
        return self.product_list_links

    async def collect_zilli(self, link):
        try:
            async with ClientSession(connector=TCPConnector(verify_ssl=False)) as session:
                async with session.get(link.attrs['href']) as response:
                    soup = BeautifulSoup(await response.text(), 'lxml')
            await asyncio.sleep(0.5)
            title = await self.get_title(soup, link)
            subtitle = await self.get_subtitle(soup, link)
            more_detail = await self.get_more_detail(soup, link)
            materials = await self.get_materials(soup, link)
            article = await self.get_article(soup, link)
            colours = "-"
            photos_links = [link.attrs['src'] for link in soup.find_all('img', {'itemprop': "image"})]
            await self.create_csv(article, title, subtitle, more_detail, materials, colours, photos_links)
        except BaseException as e:
            print(f"Критическая ошибка -- пропуск ссылки -- сайт {self.url}")
            return
