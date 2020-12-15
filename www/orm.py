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
    log(sql)
    global __pool
    # await 可以让它后面执行的语句等一会，防止多个程序同时执行，达到异步效果
    with await __pool as conn:
        # aiomysql.DictCursor 将结果作为字典返回
        # cursor 叫游标，可以用来调用execute 来执行一条单独的SQL 语句
        # 参考 https://docs.python.org/zh-cn/3.8/library/sqlite3.html?highlight=execute#cursor-objects
        cur = await conn.cursor(aiomysql.DictCursor)
        # SQL占位符是 ？，MYSQL 的占位符是 %s，select 函数在内部自动替换
        # 第一给参数传入 sql 语句并将语句中的 ？ 替换为 %s，第二给语句传入参数
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
    # 根据输入的数字创建参数个数，例如：输入3，返回 ？，？，？
    L = []
    for n in range(num):
        L.append('?')
    # 用指定的字符连接生成一个新字符串
    return ','.join(L)
# metaclass 意为元类，是类的模板，所以必须从 type 类型派生，一般用来动态的创建类
class ModelMetaclass(type):
    # __new__ 方法接收到到参数依次是
    # cls 当前准备创建的类的对象 class
    # name 类的名字 str
    # bases 类继承的父类集合 Tuple
    # attrs 类的方法合集( 通过 metaclass 动态创建的类都会将类中定义的属性以k,v 形式传入 attrs， Key 为变量名，Value 为值 )
    def __new__(cls, name, bases, attrs):
        # 排除Model 类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名称，如果要创建的类中定义了__table__ 属性，则取属性的值，如果没有或为None，则用要创建类的类名
        tableName = attrs.get('__table__', None) or name
        # 在日志里找到name 的model
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取所有的Field 和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            # 判断类型，如果 v 和 field 类型相同则返回 True ，不同则 False
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # v.primary_key 为True ，则field 为主键
                if v.primary_key:
                    # 找到主键，如果有，返回一个错误
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    # 给主键赋值
                    primaryKey = k
                else:
                    # 没有找到主键就在fields 里加上k
                    fields.append(k)
        # 如果主键为 None 就报错
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        # 如果key 存在于字典中，则将其移除并返回其值，否则返回 default
        for k in mappings.keys():
            # 根据键移除指定元素，相当于从类属性中删除该 Field 属性，否则容易造成运行时错误（实例的属性会覆盖同名属性）
            attrs.pop(k)
        # map 会将 field 的每个元素船夫 function，并返回值的新列表
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName  # 表名
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句
        # select 语句操作时需要拼接 where 条件
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields),tableName)
        # insert 语句会调用 create_args_strings 根据参数的数量拼接成（？，？，？）
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        # update 语句操作时用 join 和 map 结合，先用 map 使每一次 lambda 返回的值作为新列表，再用join 连接成（值1=？，值2=？，值3=？）
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName, ','.join(map(lambda f:'`%s` = ?' % (mappings.get(f).name or f), fields)), primaryKey)
        # delete 语句值根据主键删除
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

# metaclass 参数提示 Model 要通过上面的 __new__ 来创建，扩展 dict
class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        #  初始化 ，super 显式加点属性查找绑定过程的一部分实现
        # https://docs.python.org/zh-cn/3.8/library/functions.html?highlight=super#super
        super(Model, self).__init__(**kw)
    # 返回参数 key 的自身属性，取不到值就抛出异常
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)
    # 设置key，value 的值
    def __setattr__(self, key, value):
        self[key] = value
    # 通过属性返回想要的值
    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        # 获取某个属性的值，如果没有赋值就获取其对应列的默认值
        value = getattr(self, key, None)
        # 如果 value 为 None 定位某个键，否则直接返回
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                # 如果不为 None 则赋值给 value
                value = field.default() if callable(field.default) else field.default
                logging.debug('Using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)
        return value
    # 往 Model 类添加 class 方法，可以让所有子类调用 class 方法
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        # 通过主键找对象
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def findAll(cls, where=None, args=None, **kw):
        # find objects by where clause 根据条件查询
        sql = [cls.__select__]
        # where 默认为 None
        # 如果 where 有值在 sql 加上字符串 'where' 和变量 where
        if where:
            sql.append('where')
            sql.append(where)
        # args 默认为 None，如果findAll 未传入有效的 where ，则传入 args
        if args is None:
            args = []

        orderBy = kw.get('orderBy', None)
        if orderBy:
            # get 可以返回 orderBy 的值，如果失败返回 None，这样失败也不会出错
            # orderBy 有值时给 sql 加值，为空不动
            sql.append('order by')
            sql.append(orderBy)
        # 与 orderBy 类似
        limit = kw.get('limit',None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                # 把 limit 加到末尾
                args.extend(limit)
            else:
                # 否则报错
                raise ValueError(' Invalid limit value: %s' % str(limit))
        rs = await select(''.join(sql), args)
        # 返回选择到列表里所有的值
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        # find number by select and where
        # 找到选中的数及其位置
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(''.join(sql), args, 1)
        # 如果 rs 无元素，返回 None，有则返回某个数
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    # 往Model 类添加实例方法，可以让所有子类调用实例方法
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)

# 定义 Field ，构建属性时的父类
class Field(object):
    # __init__ 将传入的参数初始化给对象
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    # 字符输出
    def __str__(self):
        return '<$s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

# 定义Field 子类及其子类的默认值
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, primary_key, default, ddl)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)
# 在sql 中 float 可以存储4 或8 字节，而 real 存储4 个
class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'real', primary_key, default)
# text 比 varchar 容量更大，text 不允许有默认值，定义了也不生效，例如：text（200）
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)
