#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2013-09-10"

import ibm_db
from dbase import DbTools

class DB2(DbTools):
    """
    DB2数据库类
    """

    def _getConn(self):
        """
        连接数据库，获取数据库连接
        """

        conn_str = "DATABASE=%s;HOSTNAME=%s;PORT=%d;PROTOCOL=TCPIP;UID=%s;PWD=%s;" \
                % (self._name, self._host, self._port, self._user, self._pass)
        conn = ibm_db.connect(conn_str, '', '')

        if conn:
            return conn
        else:
            return None

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
        sql = sql + " fetch first " + str(self._reclimit) + " rows only"

        return sql
        #return sql.decode('utf-8')

    def getData(self):
        """
        抽取数据
        flds: 要抽取的字段列表
        curpos: 从什么位置开始抽取数据；默认为-1，从0开始读取
        fetch_tuple()：返回元组，以列的位置索引
        fetch_tuple()：返回字典，以列名索引
        fetch_both()：返回字典，以列名和列的位置做索引
        http://publib.boulder.ibm.com/infocenter/db2luw/v9r5/index.jsp?topic=%2Fcom.ibm.db2.luw.apdv.python.doc%2Fdoc%2Ft0054388.html
        注：返回的是元组列表
        """

        lst = []
        sql = self._createSql(flds, curpos)
        #print sql

        conn = self._getConn()
        if conn == None:
            return lst

        try:
            serverinfo = ibm_db.server_info( conn )
            if (serverinfo.DBMS_NAME[0:3] != 'IDS'):
                stmt = ibm_db.prepare(conn, sql, {ibm_db.SQL_ATTR_CURSOR_TYPE: ibm_db.SQL_CURSOR_KEYSET_DRIVEN})
            else:
                stmt = ibm_db.prepare(conn, sql)
            ibm_db.execute(stmt)
            tup = ibm_db.fetch_tuple( stmt )
            while (tup):
                #print tup[0]
                lst.append(tup)
                tup = ibm_db.fetch_tuple( stmt)
        except:
            raise
        finally:
            ibm_db.close(conn)

        return lst

