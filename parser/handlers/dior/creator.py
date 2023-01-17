import asyncio

import aiofiles
import pandas as pd
from jinja2 import FileSystemLoader, Environment

from parser.handlers.general import get_photo_b64


class CreatorDior:

    def __init__(self, bar):
        self.path = './parser/dior/csv'
        self.save_path = './parser/dior/html'
        self.template_path = './parser/'
        self.rate_sem = asyncio.BoundedSemaphore(100)
        self.bar = bar

    async def main_dior(self, file):
        env = Environment(loader=FileSystemLoader(f"{self.template_path}"), autoescape=True)
        template = env.get_template('dior/template_dior.html')
        df = await self.parse_csv(file)
        data = df.to_dict('list')
        images = await get_photo_b64(data['photos'], self.bar)
        data['photos'] = images
        async with aiofiles.open(f"{self.save_path}/DIOR-{file.replace('.csv', '.html')}", "w",
                                 encoding='utf-8') as f:
            await f.write(template.render(site=f"DIOR - {file.replace('.csv', '')}", data=data,
                                          count_records=len(data['article'])))
            await asyncio.sleep(0.01)

    async def parse_csv(self, file):
        converter = lambda x: x.replace('\'', '').strip("[\'\']").split(", ")
        df = pd.read_csv(f'{self.path}/{file}',
                         delimiter=';',
                         dtype={
                             'article': str,
                             'title': str,
                             'subtitle': str,
                             'size_and_fit': str,
                             'colours': str,
                         },
                         converters={"photos": converter},
                         )
        return df

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

    async def main(self, file):
        rt = asyncio.create_task(self.releaser())
        await asyncio.gather(
            *[self.delay_wrapper(self.main_dior(file))])
        rt.cancel()
