#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-09"

from oracle import Oracle
from mssql import MsSql
from mysql import MySql
#from db2 import DB2
#from sybase import SaiBase
from postgres import Postgres

class DbFactory:
    """
    数据库类工厂
    """

    def __init__(self, dbparams):
        """
        dbparams：数据库相关字典
        包括的keys有：dbhost, dbport, dbname, dbuser, dbpass
        """
        self._params = dbparams

    def factory(self, dbtype):
        dbtype = dbtype.lower()
        if dbtype == 'oracle':
            return Oracle(self._params)
        elif dbtype == 'mssql':
            return MsSql(self._params)
        elif dbtype == 'mysql':
            return MySql(self._params)
        #elif dbtype == 'db2':
        #    return DB2(self._params)
        #elif dbtype == 'sybase':
        #    return SaiBase(self._params)
        elif dbtype == 'postgresql':
            return Postgres(self._params)
        else:
            return None

def test_postgres():
    dic = {}
    dic['dbhost'] = '192.168.1.23'
    dic['dbport'] = '5432'
    dic['dbname'] = 'mdstack'
    dic['dbuser'] = 'ustack'
    dic['dbpass'] = '123465'

    dic['tbname'] = 't_resultdetail'
    dic['reclimit'] = '5'
    dic['timefld'] = 'idxname'
    dic['incrfld'] = 'rdid'
    dic['msgfld'] = 'cast(msgid as character varying) || message'

    fac = DbFactory(dic)
    db = fac.factory('postgres')
    if db != None:
        db.getData(['warnid', 'resultid', 'idxname', 'msgid'], 8)

def test_oracle():
    dic = {}
    dic['dbhost'] = '192.168.2.110'
    dic['dbport'] = '1521'
    dic['dbname'] = 'orcl'
    dic['dbuser'] = 'scott'
    dic['dbpass'] = 'tiger'

    dic['tbname'] = 'emp'
    dic['reclimit'] = '5'
    dic['timefld'] = 'hiredate'
    dic['incrfld'] = 'empno'
    dic['msgfld'] = 'ename || job as 消息'

    fac = DbFactory(dic)
    db = fac.factory('oracle')
    if db != None:
        db.getData(['ename', 'job', 'mgr', 'sal'], 7899)

def test_mssql():
    dic = {}
    dic['dbhost'] = '192.168.2.119'
    dic['dbport'] = '1433'
    dic['dbname'] = 'LSRZDB'
    dic['dbuser'] = 'sa'
    dic['dbpass'] = 'sql2005'

    dic['tbname'] = 't_rzzb'
    dic['reclimit'] = '5'
    dic['timefld'] = 'dReceive'
    dic['incrfld'] = 'cAuthno'
    dic['msgfld'] = 'cLinkman + cLinkphone as 消息'

    fac = DbFactory(dic)
    db = fac.factory('mssql')
    if db != None:
        db.getData(['cApplicant', 'cCountry', 'cClass', 'cStyle'], 11000030)

def test_mysql():
    dic = {}
    dic['dbhost'] = '192.168.2.110'
    dic['dbport'] = '3306'
    dic['dbname'] = 'wordpress'
    dic['dbuser'] = 'root'
    dic['dbpass'] = '123465'

    dic['tbname'] = 'wp_posts'
    dic['reclimit'] = '5'
    dic['timefld'] = 'post_date'
    dic['incrfld'] = 'id'
    dic['msgfld'] = 'Concat(post_title, post_content) as 消息'

    fac = DbFactory(dic)
    db = fac.factory('mysql')
    if db != None:
        db.getData(['post_status', 'comment_status', 'ping_status', 'post_type'], 25)

# 测试
if __name__ == '__main__':
    test_mysql()
