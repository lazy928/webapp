# -*- coding: utf-8 -*-
from aiohttp import web

async def index(request):
    return web.Response(text='Welcome !')

def init():
    app = web.Application()
    app.router.add_get('/', index)
    web.run_app(app, host='127.0.0.1', port=9999)

init()