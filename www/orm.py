# -*- coding: utf-8 -*-
import logging, aiomysql

# 创建连接池
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    # 全局变量__pool 存储，默认为utf8
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port',3360),
        user = kw('user'),
        password = kw('password'),
        db = kw('db'),
        charset = kw.get('charset', 'utf8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize',1),
        loop = loop
    )

