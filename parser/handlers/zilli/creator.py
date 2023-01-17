import asyncio
import aiofiles
import pandas as pd
from jinja2 import Environment, FileSystemLoader

from parser.handlers.general import get_photo_b64


class CreatorZilli:

    def __init__(self, bar):
        self.path = './parser/zilli/csv'
        self.save_path = './parser/zilli/html'
        self.template_path = './parser/'
        self.rate_sem = asyncio.BoundedSemaphore(100)
        self.bar = bar

    async def main_zilli(self, file):
        env = Environment(loader=FileSystemLoader(f"{self.template_path}"), autoescape=True)
        template = env.get_template('zilli/template_zilli.html')
        df = await self.parse_csv(file)
        data = df.to_dict('list')
        images = await get_photo_b64(data['photos'], self.bar)
        data['photos'] = images
        async with aiofiles.open(f"{self.save_path}/ZILLI-{file.replace('.csv', '.html')}", "w",
                                 encoding='utf-8') as f:
            await f.write(template.render(site=f"ZILLI - {file.replace('.csv', '')}", data=data,
                                          count_records=len(data['article'])))
            await asyncio.sleep(0.01)

    async def parse_csv(self, file):
        converter = lambda x: x.replace('\'', '').strip("[\'\']").split(", ")
        converter_mm = lambda x: x.replace(',,,', '<br>')
        df = pd.read_csv(f'{self.path}/{file}',
                         delimiter=';',
                         dtype={
                             'article': str,
                             'title': str,
                             'subtitle': str,
                         },
                         converters={"photos": converter, "more_details": converter_mm, "materials": converter_mm},
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
            *[self.delay_wrapper(self.main_zilli(file))])
        rt.cancel()
