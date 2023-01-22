import asyncio
import base64

from aiofiles import tempfile
from aiohttp import ClientSession, TCPConnector
from webptools import base64str2webp_base64str, grant_permission


async def create_b64_image(session, photo):
    async with session.get(photo) as response:
        image_in_b64 = base64.b64encode(await response.content.read())
        async with tempfile.NamedTemporaryFile() as file:
            webp_in_b64 = base64str2webp_base64str(image_in_b64.decode(), image_type='jpg', option='-q 50',
                                                   temp_path=file.name)
            await asyncio.sleep(0.1)
            return webp_in_b64[0]


async def get_photo_b64(photos):
    new_photos = []
    grant_permission()
    async with ClientSession(connector=TCPConnector(verify_ssl=False)) as session:
        for i, photo_set in enumerate(photos):
            new_photos_set = []
            for photo in photo_set:
                image = await create_b64_image(session, photo)
                new_photos_set.append(image)
            new_photos.append(new_photos_set)
            await asyncio.sleep(0.01)
    return new_photos
