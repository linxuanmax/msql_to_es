#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-09"

import inspect, sys, os
import psycopg2
import psycopg2.extras
from dbase import DbTools

class Postgres(DbTools):
    """
    Postgres数据库类
    """

    def _clspath(self):
        """
        获取当前类文件所在的路径
        """

        clsfile = inspect.getfile(inspect.currentframe())
        return os.path.abspath(os.path.dirname(clsfile))

    def _getCursor(self):
        """
        获取postgresql数据库的游标
        """

        cursor = None
        try:
            url = "host=" + self._host + " port=" + self._port  + " user=" + \
                    self._user + " password=" + self._pass + " dbname=" + self._name
            conn = psycopg2.connect(url)

            cursor = conn.cursor()
            #cursor= conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

        sql = "select " + sql + " from " + self._tbname
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
        fetchmany([size=cursor.arraysize])：返回元组列表
        fetchall()：直接返回元组列表
        http://initd.org/psycopg/docs/cursor.html
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
        
            #for r in lst:
            #    print r
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
        sql = "select case when max(" + self._timefld + ") is null then '1980-01-01 00:00:00' else to_char(max(" + self._timefld + "), 'yyyy-mm-dd hh24:mi:ss') end from " + self._tbname

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
        sql = "select case when min(" + self._timefld + ") is null then '1980-01-01 00:00:00' else to_char(min(" + self._timefld + "), 'yyyy-mm-dd hh24:mi:ss') end from " + self._tbname

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

        dic_datatype = {"abstime": "time",
                        "_abstime": "_abstime",
                        "aclitem": "aclitem",
                        "_aclitem": "_aclitem",
                        "int8": "number",
                        "_int8": "_int8",
                        "bit": "bit",
                        "_bit": "_bit",
                        "varbit": "string",
                        "_varbit": "_varbit",
                        "bool": "bool",
                        "_bool": "_bool",
                        "box": "box",
                        "_box": "_box",
                        "bytea": "bytea",
                        "_bytea": "_bytea",
                        "char": "string",
                        "_char": "_char",
                        "bpchar": "string",
                        "_bpchar": "_bpchar",
                        "varchar": "string",
                        "_varchar": "_varchar",
                        "cid": "string",
                        "_cid": "_cid",
                        "cidr": "string",
                        "_cidr": "_cidr",
                        "circle": "circle",
                        "_circle": "_circle",
                        "date": "time",
                        "_date": "_date",
                        "float8": "number",
                        "_float8": "_float8",
                        "gtsvector": "gtsvector",
                        "_gtsvector": "_gtsvector",
                        "inet": "inet",
                        "_inet": "_inet",
                        "int2vector": "int2vector",
                        "_int2vector": "_int2vector",
                        "int4": "number",
                        "_int4": "number",
                        "interval": "interval",
                        "_interval": "_interval",
                        "line": "line",
                        "_line": "_line",
                        "lseg": "lseg",
                        "_lseg": "_lseg",
                        "macaddr": "string",
                        "_macaddr": "_macaddr",
                        "money": "money",
                        "_money": "_money",
                        "name": "string",
                        "_name": "_name",
                        "numeric": "number",
                        "_numeric": "_numeric",
                        "oid": "number",
                        "_oid": "_oid",
                        "oidvector": "oidvector",
                        "_oidvector": "_oidvector",
                        "path": "path",
                        "_path": "_path",
                        "pg_node_tree": "pg_node_tree",
                        "point": "point",
                        "_point": "_point",
                        "polygon": "polygon",
                        "_polygon": "_polygon",
                        "float4": "number",
                        "_float4": "_float4",
                        "refcursor": "refcursor",
                        "_refcursor": "_refcursor",
                        "regclass": "regclass",
                        "_regclass": "_regclass",
                        "regconfig": "regconfig",
                        "_regconfig": "_regconfig",
                        "regdictionary": "regdictionary",
                        "_regdictionary": "_regdictionary",
                        "regoper": "regoper",
                        "_regoper": "_regoper",
                        "regoperator": "regoperator",
                        "_regoperator": "_regoperator",
                        "regproc": "regproc",
                        "_regproc": "_regproc",
                        "regprocedure": "regprocedure",
                        "_regprocedure": "_regprocedure",
                        "regtype": "regtype",
                        "_regtype": "_regtype",
                        "reltime": "time",
                        "_reltime": "_reltime",
                        "int4": "number",
                        "int2": "number",
                        "_int2": "_int2",
                        "smgr": "smgr",
                        "text": "string",
                        "_text": "_text",
                        "tid": "tid",
                        "_tid": "_tid",
                        "timestamp": "time",
                        "_timestamp": "_timestamp",
                        "timestamptz": "time",
                        "_timestamptz": "_timestamptz",
                        "time": "time",
                        "_time": "_time",
                        "timetz": "time",
                        "_timetz": "_timetz",
                        "tinterval": "tinterval",
                        "_tinterval": "_tinterval",
                        "tsquery": "tsquery",
                        "_tsquery": "_tsquery",
                        "tsvector": "tsvector",
                        "_tsvector": "_tsvector",
                        "txid_snapshot": "txid_snapshot",
                        "_txid_snapshot": "_txid_snapshot",
                        "uuid": "string",
                        "_uuid": "_uuid",
                        "xid": "string",
                        "_xid": "_xid",
                        "xml": "xml",
                        "_xml": "_xml"}

        # 表名区分大小写
        sql = "SELECT a.attname column_name, t.typname data_type FROM pg_class c, pg_attribute a, pg_type t"
        sql += " WHERE c.relname = '" + self._tbname + "' and a.attnum > 0 and a.attrelid = c.oid and a.atttypid = t.oid"
        sql += " ORDER BY a.attnum"
        cur = self._getCursor()

        lstFlds = []
        if cur == None:
            return None
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
