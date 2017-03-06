#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-10-17"

import os
import Sybase
from dbase import DbTools

class SaiBase(DbTools):
    """
    Sybase数据库类
    """

    def _getCursor(self):
        """
        获取postgresql数据库的游标
        """

        cursor = None
        try:
            db = Sybase.connect(self._host, self._user, self._pass, self._name)

            cursor = db.cursor()
        except:
            raise  # 原封不动的把异常抛出到上层调用代码

        return cursor

    def _createSql(self, flds, cpos):
        """
        生成抽取数据的SQL语句
        """

        fields = flds[:]
        fields.append(self._timefld)
        fields.append(self._msgfld)
        fields.append(self._incrfld)
        sql = ", ".join(fields)
        sql = "select " + sql + " from " + self._tbname
        sql = sql + " where " + self._incrfld + " > " + str(cpos)
        sql = sql + " order by " + self._incrfld
        sql = "set rowcount " + str(self._reclimit) + ";" + sql
        
        return sql

    def getData(self, flds, curpos=-1):
        """
        抽取数据
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        注：返回的是元组列表
        """

        lst = []
        sql = self._createSql(flds, curpos)
        #print sql

        cur = self._getCursor()
        if cur == None:
            return lst

        try:
            cur.execute(sql)
            lst = cur.fetchall()

            #for row in lst:
            #    print row
        except:
            raise
        finally:
            cur.close()
            cur.connection.close()

        return lst
