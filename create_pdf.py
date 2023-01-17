import argparse
import asyncio
import os

import pandas as pd
from alive_progress import alive_bar
from pyhtml2pdf import converter

from parser.handlers.dior.creator import CreatorDior
from parser.handlers.zilli.creator import CreatorZilli


class TotalCreator:

    def __init__(self):
        self.total = 0
        self.rate_sem = asyncio.BoundedSemaphore(50)

        self.path_dior_csv = './parser/dior/csv'
        self.path_dior_html = './parser/dior/html'
        self.path_dior_pdf = './parser/dior/pdf'

        self.path_zilli_csv = './parser/zilli/csv'
        self.path_zilli_html = './parser/zilli/html'
        self.path_zilli_pdf = './parser/zilli/pdf'

        self.dior_tasks = []
        self.zilli_tasks = []

    async def get_total(self, path, file, is_return=False):
        df = pd.read_csv(f'{path}/{file}',
                         delimiter=';', )
        self.total += len(df)
        if is_return:
            return len(df)

    async def start_zilli(self, bar):
        task_zilli = CreatorZilli(bar)
        for file in os.listdir(self.path_zilli_csv):
            self.zilli_tasks.append(asyncio.create_task(task_zilli.main(file)))
        await asyncio.gather(*self.zilli_tasks)

    async def start_dior(self, bar):
        task_dior = CreatorDior(bar)
        for file in os.listdir(self.path_dior_csv):
            self.dior_tasks.append(asyncio.create_task(task_dior.main(file)))
        await asyncio.gather(*self.dior_tasks)

    async def create_pdf(self, file, save_path, get_path, bar):
        path = os.path.abspath(f"{get_path}/{file}")
        converter.convert(f'file:///{path}', f"{save_path}/{file.replace('.html', '.pdf')}",
                          print_options={
                              'paperWidth': 400 / 25.4,
                              'paperHeight': 720 / 25.4,
                              'marginBottom': 0,
                              'marginLeft': 0,
                              'marginRight': 0,
                              'marginTop': 0,
                              'preferCSSPageSize': True
                          })
        bar()

    async def request_api_to_create_pdf(self, brand, bar):
        if brand == 'zilli' or brand == 'all':
            files_zilli = [file for file in os.listdir(self.path_zilli_html)]
            for file in files_zilli:
                await self.create_pdf(file, self.path_zilli_pdf, self.path_zilli_html, bar)
        if brand == 'dior' or brand == 'all':
            files_dior = [file for file in os.listdir(self.path_dior_html)]
            for file in files_dior:
                await self.create_pdf(file, self.path_dior_pdf, self.path_dior_html, bar)

    async def total_start(self, site, create=True, only_create=False):
        if site == 'all' and not only_create:
            for file in os.listdir(self.path_dior_csv):
                await self.get_total(self.path_dior_csv, file)
            for file in os.listdir(self.path_zilli_csv):
                await self.get_total(self.path_zilli_csv, file)
            with alive_bar(self.total, title='Процесс формирования каталога с сайтов \"Zilli and Dior\"',
                           theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_zilli(bar)), asyncio.create_task(self.start_dior(bar))]
                await asyncio.gather(*main_task)
        elif site == 'dior' and not only_create:
            for file in os.listdir(self.path_dior_csv):
                await self.get_total(self.path_dior_csv, file)
            with alive_bar(self.total, title='Процесс формирования каталога с сайта \"Dior\"', theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_dior(bar))]
                await asyncio.gather(*main_task)
        elif site == 'zilli' and not only_create:
            for file in os.listdir(self.path_zilli_csv):
                await self.get_total(self.path_zilli_csv, file)
            with alive_bar(self.total, title='Процесс формирования каталога с сайта \"Zilli\"', theme='smooth') as bar:
                main_task = [asyncio.create_task(self.start_zilli(bar))]
                await asyncio.gather(*main_task)
        if create or only_create:
            total = 0
            if site == 'dior' or site == 'all':
                total += len(os.listdir(self.path_dior_html))
            if site == 'zilli' or site == 'all':
                total += len(os.listdir(self.path_zilli_html))
            with alive_bar(total, title='Процесс формирования PDF', theme='smooth') as bar:
                await self.request_api_to_create_pdf(site, bar)
        else:
            return "Неизвестный аргумент"


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Парсер сайтов брендовой одежды',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s', '--site', type=str,
                        help="""Собрать каталог и создать pdf""")
    parser.add_argument('-cc', type=str,
                        help="""Сконвертировать каталог в PDF после завершения сбора\n[true/false]""")
    parser.add_argument('-ok', type=str,
                        help="""Сконвертировать каталог в PDF\n[true/false]
(ВАЖНО!!!)
Только при наличии .html файлов в каталогах брендов""")
    args = parser.parse_args()
    if not args.site:
        args.site = 'all'
    if not args.cc or args.cc == 'true':
        args.cc = True
    elif args.cc == 'false':
        args.cc = False
    if not args.ok or args.ok == 'false':
        args.ok = False
    elif args.ok == 'true':
        args.ok = True
    loop = asyncio.get_event_loop()
    total_creator = TotalCreator()
    loop.run_until_complete(total_creator.total_start(args.site, args.cc, args.ok))
    loop.close()
