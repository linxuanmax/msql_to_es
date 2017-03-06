#coding=utf-8

import os
import MySQLdb
import json


class DbTool():

    def __init__(self, dbDic):

        self._dbhost = dbDic['dbhost']
        self._dbport = dbDic['dbport']
        self._dbname = dbDic['dbname']
        self._dbuser = dbDic['dbuser']
        self._dbpass = dbDic['dbpass']

        self._taskname = dbDic['taskname'] #任务列表的名字
        self._reclimit = dbDic['reclimit'] #每次导入的限制数据数量
        self._inctype = dbDic['inctype'] #导入数据的类型
        self._timefld = dbDic['timefld'] #时间增量的字段
        self._incrfld = dbDic['incrfld'] #主键的类型
        self._msgfld = dbDic['msgfld']
        self._tbstructure = dbDic['tbstructure']
        self._tbname = dbDic['tbname']
        self._sql = dbDic['sql']
        self._miancr = dbDic['mianincr']

    def get_cursor(self):

        #获取游标
        try:
            conn = MySQLdb.Connect(host=self._dbhost, port=int(self._dbport), user=self._dbuser, passwd=self._dbpass, db=self._dbname, charset='utf8')
            cursor = conn.cursor()
        except:
            raise  # 原封不动的把异常抛出到上层调用代码
        return cursor

    def create_sql(self):

        tbstructure = ','.join(self._tbstructure)
        filed = os.path.split(os.path.realpath(__file__))[0] + '/track/' + "%s" % (self._taskname + '.json')
        task_file = open(filed, 'r')
        files = json.load(task_file)
        curpos = files['curpos']
        curpos_stime = files['curpos_stime']
        curpos_etime = files['curpos_etime']

        sql_common_one_num = ' select %s from %s where %s > %s '
        sql_common_one_str = ' select %s from %s where %s > "'"%s"'" '
        sql_common_two = 'order by %s asc limit %s'
        sql_indify = ' and %s > "'" %s "'" and %s <= "'" %s "'" '

        if self._inctype == 1:
            if self._miancr == 'number':
                sql = sql_common_one_num
            elif self._miancr == 'string':
                sql = sql_common_one_str
            else:
                sql = sql_common_one_str
            sql = sql%(tbstructure, self._tbname, self._incrfld, curpos)
            sql = sql + sql_common_two % (self._incrfld, self._reclimit) + ';'
            print sql
            task_file.close()

        elif self._inctype == 2:
            if self._miancr == 'number' :
                sql = sql_common_one_num
            elif self._miancr == 'string' :
                sql = sql_common_one_str
            else:
                sql = sql_common_one_str
            sql = sql%(tbstructure, self._tbname, self._incrfld, curpos)
            sql = sql + sql_indify%(self._msgfld, curpos_stime, self._msgfld, curpos_etime)
            sql = sql + sql_common_two % (self._incrfld, self._reclimit) + ';'
            task_file.close()

        else:
            sql = (self._sql)%(tbstructure, self._tbname)
        return sql

    def get_data(self):
        # 抽取数据
        cur = self.get_cursor()
        sql = self.create_sql()

        cur.execute(sql)
        lst = cur.fetchall()
        lst = list(lst)
        cur.close()
        return lst

    def get_num(self):
        cur = self.get_cursor()
        try:
            sql = 'select count(*) from %s'%(self._tbname)
            cur.execute(sql)
            lst_num = cur.fetchall()

        except:
            raise
        finally:
            cur.close()
        return lst_num[0][0]

