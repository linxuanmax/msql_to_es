#!usr/bin/env python
#coding=utf-8

__author__="yanzl"
__date__="2017-02-27"


from utils import sys_config, sys_log
from dbtools import globalvariable
# from dbtools import dbase_new
from tools import mysqltask, dbfactory

import os
import multiprocessing
import esdataextract_new
import base64
import time
import traceback


# 配置文件
#configFile = sys_config.getDir() + "/conf/mdstack.conf"
configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
if os.path.exists(configFile) == False:
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"


def run(start_type=None):

    # 进程号文件名及配置
    conf = sys_config.SysConfig(configFile)
    pidFile = conf.getConfig("dataextract", "pidFile")
    #把进程号写进一上文件
    if start_type is None:
        if os.path.exists(pidFile):
            os.remove(pidFile)
        pFile = open(pidFile, "w")
        pFile.write(str(os.getpid()))
        pFile.close()
    #传进任务列表（列表形式）， 数据库连接 （字典形式）
    dic_tasklist = globalvariable.dic_tasklist
    dic_connection = globalvariable.dic_connection

    #如果任务列表不为空以及列表元素大小零，执行任务
    if dic_tasklist is not None and len(dic_tasklist)>0:
        createTask(dic_tasklist, dic_connection)


def createTask(dic_tasklist, dic_connection):

    for dic in dic_tasklist:
        #任务名称
        taskName = dic["taskname"]
        #主键
        dbID = str( dic["dbid"] )
        # 每次读取的记录数量
        recLimit = dic["reclimit"]
        # 数据增量方式：1 主键增量； 2 时间增量； 0 一次性导入数据
        incType = dic["inctype"]
        # 日期字段，增量方式为2时有用
        timeFld = dic["timefld"]
        # 主键字段
        incrFld = dic["incrfld"]
        # 输出到message字段的数据拼接形式
        msgFld = dic["msgfld"]
        # 自增长字段的类型
        mianincr = dic['mianincr']
        # 读取数据的sql语句
        sql = dic['sql']
        # 数据表中字段
        tbStructure = dic['tbstructure']
        #数据表名称
        tbname = dic['tbname']


        connInfo = dic_connection[dic['conname']]

        # 把执行任务所需要的参数放进一个变量中，创建多进程执行任务
        argus = (dbID, taskName, recLimit, incType, timeFld, incrFld, msgFld, connInfo, sql, tbStructure, tbname, mianincr)
        p = multiprocessing.Process(name="Process" + dbID, target=taskFunc, args=argus)
        p.start()


def taskFunc(dbID, taskName, recLimit, incType, timeFld, incrFld, msgFld, connInfo, sql, tbStructure, tbname, mianincr):

    es = esdataextract_new.ESData(configFile)
    dbDic={}
    dbDic['dbhost'] = connInfo["hostname"]
    dbDic['dbport'] = str(connInfo["port"])
    dbDic['dbname'] = connInfo["dbname"]
    dbDic['dbuser'] = connInfo["username"]
    dbDic['dbpass'] = base64.decodestring(connInfo["userpass"])

    dbDic['taskname'] = taskName
    dbDic['reclimit'] = str(recLimit)
    dbDic['inctype'] = incType
    dbDic['timefld'] = timeFld
    dbDic['incrfld'] = incrFld
    dbDic['msgfld'] = msgFld
    dbDic['sql'] = sql
    dbDic['tbstructure'] = tbStructure
    dbDic['msgfld'] = msgFld
    dbDic['tbname'] = tbname
    dbDic['mianincr'] = mianincr

    inxName = connInfo['dbname']

    db = mysqltask.DbTool(dbDic)

    if db is not None:
        while True:
            lst = db.get_data()
            print lst
            post_data = dbfactory.Location(lst, taskName, mianincr, incType)
            if incType == 0:
                num = db.get_num()
                es.bulkInsData(lst, tbStructure, tbname, inxName)
                if len(lst) <= num:
                    break
            else:
                if len(lst) >= 1:
                    try:
                        es.bulkInsData(lst, tbStructure, tbname, inxName)
                        post_data.up_location()
                    except Exception as e:
                        raise e
                else:
                    post_data.add_location()
                    time.sleep(5)

if __name__ == '__main__':
    run()
