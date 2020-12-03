# -*- coding: utf-8 -*-

' orm 数据库连接 '

import logging, aiomysql

def log(sql):
    ' 打印sql '
    logging.info('SQL:%s' % sql)

# 异步IO 起手使用async,创建全局连接池
# pool 用法 https://aiomysql.readthedocs.io/en/latest/pool.html?highlight=create_pool
async def create_pool(loop, **kw):
    ' 创建全局的连接池 '
    logging.info('create database connection pool...')
    # 声明全局变量__pool 存储，默认为utf8
    global __pool
    # kw.get 的方式直接定义，kw[''] 的方式需要传入相应的属性
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),  # 主机号
        port = kw.get('port',3360),  # 端口号
        user = kw('user'),  # 用户名
        password = kw('password'),  # 密码
        db = kw('db'),  # 数据库
        charset = kw.get('charset', 'utf8'),  # 编码格式
        autocommit = kw.get('autocommit', True),  # 自动提交
        maxsize = kw.get('maxsize', 10),  # 最大连接数
        minsize = kw.get('minsize',1),  # 最小连接数
        loop = loop
    )

# 传入SELECT
async def select(sql, args, size=None):
    ' 执行Select '
    log(sql)
    global __pool
    # await 可以让它后面执行的语句等一会，防止多个程序同时执行，达到异步效果
    with await __pool as conn:
        # aiomysql.DictCursor 将结果作为字典返回
        # cursor 叫游标，可以用来调用execute 来执行一条单独的SQL 语句
        # 参考 https://docs.python.org/zh-cn/3.8/library/sqlite3.html?highlight=execute#cursor-objects
        cur = await conn.cursor(aiomysql.DictCursor)
        # SQL占位符是 ？，MYSQL 的占位符是 %s，select 函数在内部自动替换
        await cur.execute(sql.replace('?', '$s'), args or ())
        # fetchmany 获得size 数量的记录，否则通过fetchall 获取所有（剩余）数量记录
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        # close 关闭cursor
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

# 用execute 替代INSERT UPDATE DELETE 语句，返回一个整数表示影响的行数
# execute 与 select 不同的是cursor 不返回结果集，而是通过rowcount 返回结果数
async def execute(sql, args):
    log(sql)
    global __pool
    with await __pool as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            # rowcount 获取函数影响的行数
            affected = cur.rowcount
            await cur.close()
        # 错误抛出，e 改掉就不报错
        except BaseException as e:
            raise
        # 返回行数
        return affected

# 这个函数只在Model 类里被调用
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)

class ModelMetaclass(type):
    # __new__ 方法接收到到参数依次是
    # cls 当前准备创建的类的对象 class
    # name 类的名字 str
    # bases 类继承的父类集合 Tuple
    # attrs 类的方法合集
    def __new__(cls, name, bases, attrs):
        # 排除Model 类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称
        tableName = attrs.get('__table__', None) or name
        # 在日志里找到name 的model
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field 和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键，如果有，返回一个错误
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    # 没有找到主键就在fields 里加上k
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields),primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName, ','.join(map(lambda f:'`%s` = ?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

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

    # 往Model 类添加class 方法，可以让所有子类地调用class 方法
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

# 定义 Field
class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<$s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)
# 定义Field 子类及其子类的默认值
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, primary_key, default, ddl)
