#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-09"

class DbTools:
    """
    数据库基类
    """
    
    def __init__(self, dbparams):
        """
        dbparams：参数字典
        包括的keys有：
        dbhost, dbport, dbname, dbuser, dbpass （数据连接相关）
        tbname, reclimit, inctype, timefld, incrfld, msgfld  （获取数据相关）
        """

        self._host = dbparams['dbhost']
        self._port = dbparams['dbport']
        self._name = dbparams['dbname']
        self._user = dbparams['dbuser']
        self._pass = dbparams['dbpass']

        self._tbname = dbparams['tbname']
        self._reclimit = dbparams['reclimit']
        self._inctype = dbparams['inctype']
        self._timefld = dbparams['timefld']
        self._incrfld = dbparams['incrfld']
        self._msgfld = dbparams['msgfld']

    def getData(self, flds, curpos = -1):
        """
        抽取数据 
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        """
        
        pass

