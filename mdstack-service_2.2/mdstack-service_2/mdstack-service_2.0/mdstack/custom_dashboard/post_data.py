# -*- coding=utf-8 -*-

import psycopg2
import psycopg2.extras
from datetime import *
import os
import traceback
from mdstack.utils import sys_config, sys_log

def get_expiry_date(url, flag):
    """
    取ES索引数据有效天数
    """

    conn = psycopg2.connect(url)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    if flag == "logindexer":
        sql = "select * from t_settings where skey = 'logs_retain';"
    elif flag == "flows":
        sql = "select * from t_settings where skey = 'flows_retain';"
    cur.execute(sql)
    rs = cur.fetchall()
    cur.close()
    cur.connection.close()

    return int(rs[0]['svalue'])


