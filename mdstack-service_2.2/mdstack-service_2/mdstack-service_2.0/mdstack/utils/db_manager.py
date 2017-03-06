#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-08-23"

import psycopg2
import psycopg2.extras
from DBUtils.PooledDB import PooledDB

class PostgreDBManager:
    """
    PostgreSQL数据库，非连接池方式
    """

    def __init__(self, dbuser, dbpwd, dbhost, dbport, dbname):
        """
        构造函数
        """

        self.dbUser = dbuser
        self.dbPwd = dbpwd
        self.dbHost = dbhost
        self.dbPort = dbport
        self.dbName = dbname

    def getCursor(self):
        """
        取数据库连接
        """

        url = "host=" + self.dbHost + " port=" + self.dbPort  + " user=" + self.dbUser + " password=" + self.dbPwd + " dbname=" + self.dbName
        conn = psycopg2.connect(url)

        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        #cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        return cur

class PostgresDBPoolManager:
    """
    PostgreSQL数据库, 连接池方式
    """

    def __init__(self, dbuser, dbpwd, dbhost, dbport, dbname, poolmax):
        """
        构造函数
        """

        dbUser = dbuser
        dbPwd = dbpwd
        dbHost = dbhost
        dbPort = dbport
        dbName = dbname
        poolMax = int(poolmax)

        self.params = {
            'creator' : psycopg2,
            'mincached' : 0,            #启动时开启的空连接数量(缺省值 0 意味着开始时不创建连接) 
            'maxcached' : poolMax,      #连接池使用的最多连接数量(缺省值 0 代表不限制连接池大小)
            'maxshared' : 1,            #最大允许的共享连接数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量，被请求为共享的连接将会被共享使用
            'maxconnections': 0,        #最大允许连接数量(缺省值 0 代表不限制) 
            #'maxusage' : 5000,         #单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用)。当达到最大数值时，连接会自动重新连接(关闭和重新打开)
            'host' : dbHost,
            'port' : dbPort,
            'user' : dbUser,
            'password' : dbPwd,
            'database' : dbName,
            'failures' : (psycopg2.InterfaceError,)
        }

    def createDBPool():
        return PooledDB(**self.params)

    
    @staticmethod
    def getConn(dbpool):
        """
        静态函数, 从连接池中取连接
        """

        conn = None
        conn = dbpool.connection()

        return conn

    @staticmethod
    def closeDBPool(dbpool):
        """
        静态函数, 关闭连接池
        """

        if dbpool <> None:
            dbpool.close()

