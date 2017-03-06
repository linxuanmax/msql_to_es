#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-09"

import os
import cx_Oracle
from dbase import DbTools
import datetime

class Oracle(DbTools):
    """
    Oracle数据库类
    """

    def _OutputTypeHandler(self, cursor, name, defaultType, size, precision, scale):
        if defaultType in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
            return cursor.var(unicode, size, cursor.arraysize)

    def _getCursor(self):
        """
        获取postgresql数据库的游标
        """

        cursor = None
        try:
            os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
            #url = self._user + "/" + self._pass + "@" + self._host + ":" + self._port + "/" + self._name
            #conn = cx_Oracle.connect(url)
            dsn = cx_Oracle.makedsn(self._host, int(self._port), self._name)
            conn = cx_Oracle.connect(self._user, self._pass, dsn)
            conn.outputtypehandler = self._OutputTypeHandler
            #print conn.version

            cursor = conn.cursor()
        except:
            raise  # 原封不动的把异常抛出到上层调用代码

        return cursor

    def _createSql(self, flds, incrfld_type, curpos, curpos_stime, curpos_etime):
        """
        生成抽取数据的SQL语句
        """

        fields = flds[:]
        fields.append(self._incrfld + " " + self._incrfld + "___tmp")   # 注：oracle与sqlserver中，排序的字段不能在查询字段中重复出现，如select id, fld1, id order by id 
        #fields.append(self._msgfld)    廊坊版本不用拼msg.message
        sql = ", ".join(fields)

        sql = "select " + sql + " from " + self._tbname
        if incrfld_type == "number":
            sql = sql + " where " + self._incrfld + " > " + str(curpos)
        elif incrfld_type == "string":
            sql = sql + " where " + self._incrfld + " > '" + str(curpos) + "'"
        elif incrfld_type == "time":
            sql = sql + " where " + self._incrfld + " > to_date('" + str(curpos) + "', 'yyyy-mm-dd hh24:mi:ss')"

        if self._inctype == 2:
            sql = sql + " and " + self._timefld + " between to_date('" + curpos_stime + "', 'yyyy-mm-dd hh24:mi:ss') and to_date('" + curpos_etime + "', 'yyyy-mm-dd hh24:mi:ss')"
        sql = sql + " order by " + self._incrfld + " asc"

        sql = "select * from ( " + sql + " ) where ROWNUM <= "
        sql = sql + str(self._reclimit) #+ " ORDER BY ROWNUM ASC"
        return sql

    def getData(self, flds, incrfld_type, curpos=None, curpos_stime=None, curpos_etime=None):
        """
        抽取数据
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        fetchall()：直接返回元组列表
        Cursor.fetchmany([numRows=cursor.arraysize])：返回元组列表
        https://823409.net/b/http://cx-oracle.sourceforge.net/html/cursor.html
        注：返回的是元组列表
        """

        lst = []
        sql = self._createSql(flds, incrfld_type, curpos, curpos_stime, curpos_etime)
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

    def getMaxTime(self):
        """
        取时间增量字段的最大时间
        """

        if self._inctype != 2:
            return None

        maxtime = '1980-01-01 00:00:00'
        today = datetime.datetime.now()
        today = today.strftime('%Y-%m-%d') + " 23:59:59"

        # 表名不区分大小写
        sql = "select nvl(to_char(max(" + self._timefld + "), 'yyyy-mm-dd hh24:mi:ss'), '1980-01-01 00:00:00') from " + self._tbname
        sql += " where " + self._timefld + " <= to_date('" + today + "', 'yyyy-mm-dd hh24:mi:ss')"
        
        cur = self._getCursor()
        if cur == None:
            return maxtime

        try:
            cur.execute(sql)
            maxtime = (cur.fetchall())[0][0]
        except:
            raise
        finally:
            cur.close()
            cur.connection.close()

        return maxtime

    def getMinTime(self):
        """
        取时间增量字段的最小时间
        """

        if self._inctype != 2:
            return None

        mintime = '1980-01-01 00:00:00'
        today = datetime.datetime.now()
        today = today.strftime('%Y-%m-%d') + " 23:59:59"

        # 表名不区分大小写
        sql = "select nvl(to_char(min(" + self._timefld + "), 'yyyy-mm-dd hh24:mi:ss'), '1980-01-01 00:00:00') from " + self._tbname

        cur = self._getCursor()
        if cur == None:
            return mintime

        try:
            cur.execute(sql)
            mintime = (cur.fetchall())[0][0]
        except:
            raise
        finally:
            cur.close()
            cur.connection.close()

        return mintime

    def getTableStructure(self):
        """
        取表结构
        """

        dic_datatype = {"BLOB": "string",
                        "DATE": "time",
                        "LONG": "number",
                        "NUMBER": "number",
                        "CLOB": "string",
                        "VARCHAR2": "string",
                        "RAW": "string",
                        "LONG RAW": "string"}

        # 表名为大写
        # USER_TAB_COLS or ALL_TAB_COLS
        sql = "select COLUMN_NAME column_name, DATA_TYPE data_type from USER_TAB_COLS where TABLE_NAME='" + self._tbname.upper() + "' "
        cur = self._getCursor()

        lstFlds = []
        if cur == None:
            return lstFlds
        try:
            cur.execute(sql)
            rs = cur.fetchall()
            for flds in rs:
                for k in dic_datatype:
                    if (flds[1]).lower() == k.lower() and (not flds[0].startswith("SYS_") or not flds[0].endswith('$')):
                        lstFlds.append({"fldid": None, "fldsrc": flds[0], "fldout": dic_datatype[k]})
        except:
            raise
        finally:
            cur.close()
            cur.connection.close()
        return lstFlds
