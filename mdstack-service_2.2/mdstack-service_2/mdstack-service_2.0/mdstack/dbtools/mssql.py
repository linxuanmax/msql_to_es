#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-11"

import pymssql
from dbase import DbTools

class MsSql(DbTools):
    """
    SQL Server数据库类
    """

    def _getCursor(self):
        """
        获取SQL Server数据库的游标
        """

        cursor = None
        try:
            conn = pymssql.connect(host=self._host + ":" + self._port, user=self._user, \
                    password=self._pass, database=self._name, charset="utf8")

            cursor = conn.cursor()
        except:
            raise  # 原封不动的把异常抛出到上层调用代码

        return cursor

    def _createSql(self, flds, incrfld_type, curpos, curpos_stime, curpos_etime):
        """
        生成抽取数据的SQL语句
        """

        fields = flds[:]
        fields.append(self._incrfld + " " + self._incrfld + "___tmp")    # 注：oracle与sqlserver中，排序的字段不能在查询字段中重复出现，如select id, fld1, id order by id
        #fields.append(self._msgfld)    廊坊版本不用拼msg.message
        sql = ", ".join(fields)
        
        sql = "select top " + str(self._reclimit) + sql
        sql = sql + " from " + self._tbname
        if incrfld_type == "number":
            sql = sql + " where " + self._incrfld + " > " + str(curpos)
        elif incrfld_type == "string":
            sql = sql + " where " + self._incrfld + " > '" + str(curpos) + "'"
        elif incrfld_type == "time":
            sql = sql + " where " + self._incrfld + " > '" + str(curpos) + "'"

        if self._inctype == 2:
            sql = sql + " and " + self._timefld + " between '" + curpos_stime + "' and '" + curpos_etime + "'"
        sql = sql + " order by " + self._incrfld + " asc"
        
        return sql

    def getData(self, flds, incrfld_type, curpos=None, curpos_stime=None, curpos_etime=None):
        """
        抽取数据
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        fetchall()：直接返回元组列表
        fetchmany(size=None)：返回元组列表
        http://code.google.com/p/pymssql/wiki/PymssqlModuleReference
        注：返回的是元组列表
        返回中文时可以用str(lst[0]).decode("utf8")
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
            cur.connection.close()
            cur.close()

        return lst

    def getMaxTime(self):
        """
        取时间增量字段的最大时间
        """

        if self._inctype != 2:
            return None

        maxtime = '1980-01-01 00:00:00'
        # 表名不区分大小写
        sql = "select isnull(convert(varchar(20), max(" + self._timefld + "), 20), '1980-01-01 00:00:00') from " + self._tbname

        cur = self._getCursor()
        if cur == None:
            return maxtime

        try:
            cur.execute(sql)
            maxtime = (cur.fetchall())[0][0]
        except:
            raise
        finally:
            cur.connection.close()
            cur.close()

        return maxtime

    def getMinTime(self):
        """
        取时间增量字段的最小时间
        """

        if self._inctype != 2:
            return None

        mintime = '1980-01-01 00:00:00'
        # 表名不区分大小写
        sql = "select isnull(convert(varchar(20), min(" + self._timefld + "), 20), '1980-01-01 00:00:00') from " + self._tbname

        cur = self._getCursor()
        if cur == None:
            return mintime

        try:
            cur.execute(sql)
            mintime = (cur.fetchall())[0][0]
        except:
            raise
        finally:
            cur.connection.close()
            cur.close()

        return mintime

    def getTableStructure(self):
        """
        取表结构
        """

        dic_datatype = {"bigint": "number",
                        "binary": "string",
                        "bit": "bit",
                        "char": "string",
                        "date": "time",
                        "datetime": "time",
                        "datetime2": "time",
                        "datetimeoffset": "time",
                        "decimal": "decimal",
                        "float": "number",
                        "geography": "geography",
                        "geometry": "geometry",
                        "hierarchyid": "hierarchyid",
                        "image": "string",
                        "int": "number",
                        "money": "decimal",
                        "nchar": "string",
                        "ntext": "string",
                        "numeric": "decimal",
                        "nvarchar": "string",
                        "real": "number",
                        "smalldatetime": "time",
                        "smallint": "number",
                        "smallmoney": "decimal",
                        "sql_variant": "string",
                        "text": "string",
                        "time": "time",
                        "timestamp": "string",
                        "tinyint": "number",
                        "uniqueidentifier": "string",
                        "varbinary": "string",
                        "varchar": "string",
                        "xml": "string"}

        # 表名不区分大小写
        sql = "SELECT syscolumns.name column_name, systypes.name data_type FROM syscolumns, systypes"
        sql += " WHERE syscolumns.xusertype = systypes.xusertype AND syscolumns.id = object_id('" + self._tbname + "') "
        cur = self._getCursor()

        lstFlds = []
        if cur == None:
            return lstFlds
        try:
            cur.execute(sql)
            rs = cur.fetchall()
            for flds in rs:
                for k in dic_datatype:
                    if (flds[1]).lower() == k.lower():
                        lstFlds.append({"fldid": None, "fldsrc": flds[0], "fldout": dic_datatype[k]})
        except:
            raise
        finally:
            cur.connection.close()
            cur.close()

        return lstFlds
