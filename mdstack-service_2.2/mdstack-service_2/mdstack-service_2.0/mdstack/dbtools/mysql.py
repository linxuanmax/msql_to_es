#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-13"

import sys
import MySQLdb
from dbase import DbTools

class MySql(DbTools):
    """
    MySQL数据库类
    """

    def _getCursor(self):
        """
        获取MySql数据库的游标
        """

        cursor = None

        reload(sys)
        sys.setdefaultencoding('utf-8')

        try:
            conn = MySQLdb.Connect(host=self._host, port=int(self._port), user=self._user, \
                    passwd=self._pass, db=self._name, charset='utf8')
            cursor = conn.cursor()
        except:
            raise  # 原封不动的把异常抛出到上层调用代码

        return cursor

    def _createSql(self, flds, incrfld_type, curpos, curpos_stime, curpos_etime):
        """
        生成抽取数据的SQL语句
        """

        fields = flds[:]
        fields.append(self._incrfld)
        #fields.append(self._msgfld)    廊坊版本不用拼msg.message
        sql = ", ".join(fields)

        sql = "select " +  sql + " from " + self._tbname
        if incrfld_type == "number":
            sql = sql + " where " + self._incrfld + " > " + str(curpos)
        elif incrfld_type == "string":
            sql = sql + " where " + self._incrfld + " > '" + str(curpos) + "'"
        elif incrfld_type == "time":
            sql = sql + " where " + self._incrfld + " > '" + str(curpos) + "'"

        if self._inctype == 2:
            sql = sql + " and " + self._timefld + " between '" + curpos_stime + "' and '" + curpos_etime + "'"
        sql = sql + " order by " + self._incrfld + " asc"
        sql = sql + " limit " + str(self._reclimit)
        
        return sql

    def getData(self, flds, incrfld_type, curpos=None, curpos_stime=None, curpos_etime=None):
        """
        抽取数据
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        fetchall()：直接返回元组列表
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
        # 表名区分大小写
        sql = "select ifnull(date_format(max(" + self._timefld + "), '%Y-%m-%d %H:%i:%s'), '1980-01-01 00:00:00') from " + self._tbname

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
        # 表名区分大小写
        sql = "select ifnull(date_format(min(" + self._timefld + "), '%Y-%m-%d %H:%i:%s'), '1980-01-01 00:00:00') from " + self._tbname

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

        dic_datatype = {"bit": "string",
                        "tinyint": "number",
                        "smallint": "number",
                        "mediumint": "number",
                        "int": "number",
                        "bigint": "number",
                        "float": "number",
                        "decimal": "number",
                        "date": "time",
                        "time": "time",
                        "timestamp": "time",
                        "datetime": "time",
                        "year": "number",
                        "char": "string",
                        "varchar": "string",
                        "binary": "string",
                        "varbinary": "string",
                        "tinyblob": "string",
                        "blob": "string",
                        "mediumblob": "string",
                        "longblob": "string",
                        "tinytext": "string",
                        "text": "string",
                        "mediumtext": "string",
                        "longtext": "string",
                        "enum": "enum",
                        "set": "set",
                        "geometry": "geometry",
                        "point": "point",
                        "linestring": "string",
                        "polygon": "polygon",
                        "multipoint": "multipoint",
                        "multilinestring": "multilinestring",
                        "multipolygon": "multipolygon",
                        "geometrycollection": "geometrycollection"}

        # 表名区分大小写
        sql = "select COLUMN_NAME column_name, COLUMN_TYPE data_type from information_schema.columns where table_name='" + self._tbname + "' "
        cur = self._getCursor()

        lstFlds = []
        if cur == None:
            return lstFlds
        try:
            cur.execute(sql)
            rs = cur.fetchall()
            for flds in rs:
                for k in dic_datatype:
                    if ((flds[1]).lower()).startswith(k.lower()):
                        lstFlds.append({"fldid": None, "fldsrc": flds[0], "fldout": dic_datatype[k]})
        except:
            raise
        finally:
            cur.connection.close()
            cur.close()

        return lstFlds
