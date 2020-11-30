# -*- coding: utf-8 -*-
import logging, aiomysql



# 创建全局连接池
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

# 传入SELECT
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with await __pool as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        # SQL占位符是 ？，MYSQL 的占位符是 %s，select() 函数在内部自动替换
        await cur.execute(sql.replace('?', '$s'), args or ())
        # 通过fetchmany()获得指定数量记录，否则通过fetchall()获取所有记录
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

# 用execute 替代INSERT UPDATE DELETE 语句，返回一个整数表示影响的行数
# execute 与 select 不同的是cursor 不返回结果集，而是通过rowcount 返回结果数
async def execute(sql, args):
    log(sql)
    with await __pool as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

# 定义Model
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('Using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return value

class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<$s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

