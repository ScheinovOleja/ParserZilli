import asyncio
import base64
from aiohttp import ClientSession


async def get_photo_b64(photos, bar):
    new_photos = []
    async with ClientSession() as session:
        for i, photo_set in enumerate(photos):
            new_photos_set = []
            for photo in photo_set:
                async with session.get(photo) as response:
                    image_in_b64 = base64.b64encode(await response.content.read())
                    new_photos_set.append(image_in_b64.decode())
            new_photos.append(new_photos_set)
            bar()
            await asyncio.sleep(0.01)
    return new_photos
