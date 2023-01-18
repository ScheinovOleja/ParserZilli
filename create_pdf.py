import argparse
import asyncio
import os
import threading

import aiofiles
import numpy as np
import pandas as pd
from alive_progress import alive_bar
from jinja2 import Environment, FileSystemLoader
from pyhtml2pdf import converter

from parser.handlers.general import get_photo_b64


class TotalCreator:

    def __init__(self):
        self.total = 0

        self.path_dior_csv = './parser/dior/csv'
        self.path_dior_html = './parser/dior/html'
        self.path_dior_pdf = './parser/dior/pdf'
        self.dior_template = './parser/dior/'

        self.path_zilli_csv = './parser/zilli/csv'
        self.path_zilli_html = './parser/zilli/html'
        self.path_zilli_pdf = './parser/zilli/pdf'
        self.zilli_template = './parser/zilli/'

        self.env_dior = Environment(loader=FileSystemLoader(f"{self.dior_template}"), autoescape=True)
        self.template_dior = self.env_dior.get_template('template_dior.html')

        self.env_zilli = Environment(loader=FileSystemLoader(f"{self.zilli_template}"), autoescape=True)
        self.template_zilli = self.env_zilli.get_template('template_zilli.html')
        self.create, self.only_create = bool, bool
        self.all_tasks = []

    def parse_csv_dior(self, file):
        return pd.read_csv(f'{self.path_dior_csv}/{file}',
                           delimiter=';',
                           dtype={
                               'article': str,
                               'title': str,
                               'subtitle': str,
                               'size_and_fit': str,
                               'colours': str,
                           },
                           converters={"photos": lambda x: x.replace('\'', '').strip("[\'\']").split(", ")},
                           )

    def parse_csv_zilli(self, file):
        return pd.read_csv(f'{self.path_zilli_csv}/{file}',
                           delimiter=';',
                           dtype={
                               'article': str,
                               'title': str,
                               'subtitle': str,
                           },
                           converters={"photos": lambda x: x.replace('\'', '').strip("[\'\']").split(", "),
                                       "more_details": lambda x: x.replace(',,,', '<br>'),
                                       "materials": lambda x: x.replace(',,,', '<br>')},
                           )

    @staticmethod
    async def general_treatment(data, file, i, site, path_html, path_pdf, template):
        images = await get_photo_b64(data['photos'])
        data['photos'] = images
        async with aiofiles.open(f"{path_html}/{site.upper()}-{file.replace('.csv', f'--{i}.html')}", "w",
                                 encoding='utf-8') as html_file:
            await html_file.write(template.render(site=f"{site.upper()} - {file.replace('.csv', '')}", data=data,
                                                  count_records=len(data['article'])))
        return True

    @staticmethod
    async def create_pdf(path_pdf, site, file, html_file):
        save_path = os.path.abspath(f"{path_pdf}/{site.upper()}-{file.replace('.html', f'.pdf')}")
        converter.convert(f'file:///{os.path.abspath(html_file.name)}', f"{save_path}",
                          print_options={
                              'paperWidth': 400 / 25.4,
                              'paperHeight': 720 / 25.4,
                              'marginBottom': 0,
                              'marginLeft': 0,
                              'marginRight': 0,
                              'marginTop': 0,
                              'preferCSSPageSize': True
                          },
                          power=4)

    def start(self, file, site):
        if not self.only_create:
            if site == 'dior':
                data = self.parse_csv_dior(file)
            if site == 'zilli':
                data = self.parse_csv_zilli(file)
            count_chunks = 7 if len(data) <= 150 else 20
            chunks = np.array_split(data, count_chunks)
            tasks = []
            print('Начал работу над html')
            for i, new_data in enumerate(chunks):
                args = self.general_treatment(new_data.to_dict('list'), file, i, 'ZILLI',
                                              self.path_zilli_html, self.path_zilli_pdf,
                                              self.template_zilli, ) if site == 'zilli' else self.general_treatment(
                    new_data.to_dict('list'), file, i, 'DIOR',
                    self.path_dior_html, self.path_dior_pdf,
                    self.template_dior, )
                task = threading.Thread(target=asyncio.run, args=(args,))
                tasks.append(task)
            for task in tasks:
                task.start()
            for task in tasks:
                task.join()
            print('Начал работу над PDF')
            del chunks, args, count_chunks, data, new_data
        if self.create or (self.only_create and self.create):
            tasks = []
            for html_file in os.listdir(self.path_dior_html) if site == 'dior' else os.listdir(self.path_zilli_html):
                new_args = self.create_pdf(
                    self.path_dior_pdf, site, html_file,
                    open(f"{self.path_dior_html}/{html_file}")) if site == 'dior' else self.create_pdf(
                    self.path_zilli_pdf, site, html_file,
                    open(f"{self.path_zilli_html}/{html_file}"))
                task = threading.Thread(target=asyncio.run, args=(new_args,))
                tasks.append(task)
                if len(tasks) == 5:
                    for task in tasks:
                        task.start()
                    for task in tasks:
                        task.join()
                    tasks = []

    def total_start(self, site, create, only_create):
        self.create, self.only_create = create, only_create
        if site == 'all':
            self.total += len(os.listdir(self.path_dior_csv))
            self.total += len(os.listdir(self.path_zilli_csv))
            with alive_bar(self.total, title='Процесс формирования каталога с сайтов \"Zilli and Dior\"',
                           theme='smooth') as bar:
                for file in os.listdir(self.path_dior_csv):
                    self.start(file, 'dior')
                    bar()
                for file in os.listdir(self.path_zilli_csv):
                    self.start(file, 'zilli')
                    bar()
        if site == 'dior':
            self.total += len(os.listdir(self.path_dior_csv))
            with alive_bar(self.total, title='Процесс формирования каталога с сайта \"Dior\"', theme='smooth') as bar:
                for file in os.listdir(self.path_dior_csv):
                    self.start(file, 'dior')
                    bar()
        elif site == 'zilli':
            self.total += len(os.listdir(self.path_zilli_csv))
            with alive_bar(self.total, title='Процесс формирования каталога с сайта \"Zilli\"', theme='smooth') as bar:
                for file in os.listdir(self.path_zilli_csv):
                    self.start(file, 'zilli')
                    bar()
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
    total_creator = TotalCreator()
    total_creator.total_start(args.site, args.cc, args.ok)
