# -*- coding: utf-8 -*-

from aiohttp import web

async def index(request):
    return web.Response(text='Welcome to the hell !!')

app = web.Application()
app.add_routes([web.get('/', index)])
web.run_app(app, host='127.0.0.1', port=9999)

# loop = asyncio.get_event_loop()
# loop.run_until_complete(init())
# loop.run_forever()
