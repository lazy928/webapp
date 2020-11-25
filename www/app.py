# -*- coding: utf-8 -*-
# 教程原代码
# import logging; logging.basicConfig(level=logging.INFO)
#
# import asyncio, os, json, time
# from datetime import datetime
#
# from aiohttp import web
#
# def index(request):
#     return web.Response(body=b'<h1>Awesome</h1>')
#
# @asyncio.coroutine
# def init(loop):
#     app = web.Application(loop=loop)
#     app.router.add_route('GET', '/', index)
#     srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
#     logging.info('server started at http://127.0.0.1:9000...')
#     return srv
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(init(loop))
# loop.run_forever()

# 教程内asynico 和aiohttp 已经因python版本更新
# aiohttp 参考 http://demos.aiohttp.org/en/latest/tutorial.html
# logging 模块用于输出运行日志，可以设置输出日志的等级、日志保存路径等
# 参考 https://zhuanlan.zhihu.com/p/56968001

from aiohttp import web

async def index(request):
    # 请求--》web --》 响应（ request --》 response ）
    return web.Response(text='Welcome to the hell !')

def init():
    # 创建web app 的骨架
    app = web.Application()
    app.router.add_get('/', index)
    web.run_app(app, host='127.0.0.1', port=9999)

init()