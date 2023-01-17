import argparse
import asyncio

from alive_progress import alive_bar

from parser.handlers.dior.collector import ParserDior
from parser.handlers.zilli.collector import ParserZilli


class TotalParse:

    def __init__(self):
        self.total_links = 0
        self.zilli_tasks = []
        self.dior_tasks = []
        self.links_zilli = ["https://www.zilli.com/en/3-ready-to-wear", "https://www.zilli.com/en/12-shoes",
                            "https://www.zilli.com/en/10-accessories"]
        self.links_dior = [
            (
                "https://kpgnq6fji9-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20"
                "(4.13.1)%3B%20Browser",
                'bags'),
            (
                "https://kpgnq6fji9-dsn.algolia.net/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20"
                "(4.13.1)%3B%20Browser",
                'belts')
        ]

    async def start_zilli(self, bar):
        for link_zilli in self.links_zilli:
            task_zilli = ParserZilli(link_zilli, bar)
            self.zilli_tasks.append(asyncio.create_task(task_zilli.main()))
        await asyncio.gather(*self.zilli_tasks)

    async def start_dior(self, bar):
        for link in self.links_dior:
            task_dior = ParserDior(link, bar)
            self.dior_tasks.append(asyncio.create_task(task_dior.main()))
        await asyncio.gather(*self.dior_tasks)

    async def total_start(self, site):
        if site == 'all':
            for link in self.links_dior:
                task_dior = ParserDior(link, None, False)
                await task_dior.get_links()
                self.total_links += len(task_dior.product_list)
                del task_dior
            for link in self.links_zilli:
                task_zilli = ParserZilli(link, None, False)
                await task_zilli.get_links()
                self.total_links += len(task_zilli.product_list_links)
                del task_zilli
            with alive_bar(self.total_links, title='Процесс сбора с сайта \"Zilli and Dior\"', theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_zilli(bar)), asyncio.create_task(self.start_dior(bar))]
                await asyncio.gather(*main_task)
        elif site == 'dior':
            for link in self.links_dior:
                task_dior = ParserDior(link, None, False)
                await task_dior.get_links()
                self.total_links += len(task_dior.product_list)
                del task_dior
            with alive_bar(self.total_links, title='Процесс сбора с сайта \"Dior\"', theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_dior(bar))]
                await asyncio.gather(*main_task)
        elif site == 'zilli':
            for link in self.links_zilli:
                task_zilli = ParserZilli(link, None, False)
                await task_zilli.get_links()
                self.total_links += len(task_zilli.product_list_links)
                del task_zilli
            with alive_bar(self.total_links, title='Процесс сбора с сайта \"Zilli\"', theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_zilli(bar))]
                await asyncio.gather(*main_task)
        else:
            return "Неизвестный аргумент"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Парсер сайтов брендовой одежды',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s', '--site', type=str,
                        help="""Если хотите спарсить только 1 сайт, то введите его название.
Возможные варианты: [dior, zilli]
Команда вводится следующим образом: python main.py -s dior/zilli
Пропустить ввод сайта, если нужно спарсить всё""")
    args = parser.parse_args()
    if not args.site:
        args.site = 'all'
    loop = asyncio.get_event_loop()
    total_parser = TotalParse()
    loop.run_until_complete(total_parser.total_start(args.site))
    loop.close()
