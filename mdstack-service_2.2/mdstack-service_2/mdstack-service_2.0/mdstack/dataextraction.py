#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-09-16"

import os
import time
import traceback
import datetime
import base64

import multiprocessing

import esdataextract
import postgresdataextract
from utils import sys_config, sys_log
from dbtools import dbfactory

# TODO
# 该提示在实际部署时要删除
# 数据库的用户密码是经过base64编码过的，需要解码

# 配置文件
#configFile = sys_config.getDir() + "/conf/mdstack.conf"
configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
if os.path.exists(configFile) == False:
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
# 计划任务字典
dicTask = {}
# postgres数据处理对象
postdata = None
# 取数据时，延时长度
DELAYTIME = 10
# 循环检测数据源配置时间间隔
LOOPTIME = 60


def taskFunc( dbid, conname, tbname, idxname, reclimit, inctype, timefld, incrfld, msgfld, curpos, curpos_stime, curpos_etime ):
    """
    数据提取进程实际功能
    """

    # 提取数据库连接相关信息
    # 返回值为None或有以下键的字典( conname, conntype, hostname, port, dbname, username, userpass, usepooling )
    connInfo = postdata.getConnInfo( conname )

    es = esdataextract.ESData( configFile )

    dbDic = {}
    dbDic['dbhost'] = connInfo["hostname"]
    dbDic['dbport'] = str(connInfo["port"])
    dbDic['dbname'] = connInfo["dbname"]
    dbDic['dbuser'] = connInfo["username"]
    #dbDic['dbpass'] = connInfo["userpass"]
    dbDic['dbpass'] = base64.decodestring(connInfo["userpass"])

    dbDic['tbname'] = tbname
    dbDic['reclimit'] = str( reclimit )
    dbDic['inctype'] = inctype
    dbDic['timefld'] = timefld
    dbDic['incrfld'] = incrfld
    dbDic['msgfld'] = msgfld

    dbfac = dbfactory.DbFactory( dbDic )
    db = dbfac.factory( connInfo["conntype"] )

    # 读取要提取的数据库字段
    lstFlds = []
    n = 0
    while len(lstFlds) == 0:
        # 返回值为None或有以下键的字典列表( fldid, fldsrc, fldout )
        lstFlds = db.getTableStructure()
        n += 1
        if n == 12:
            n = 0
            conf = sys_config.SysConfig( configFile )
            _logFile = conf.getConfig( "dataextract", "logFile" )
            _instance = conf.getConfig( "dataextract", "instanceName" )
            sys_log.SysLog( _logFile, _instance ).writeLog( "error", "tbname: " + tbname + "  Not found fields!" )
        time.sleep(5)
    # 保存字段及数据类型，此功能以后应改为由web页面实现
    postdata.saveDbFieldList(dbid, lstFlds)
    # 如果需要修改导入的字段，则手工改t_dbfields表，并用下面的方法
    #lstFlds = postdata.getDbFieldList(dbid)

    flds = []
    for fld in lstFlds:
        flds.append( fld["fldsrc"] )
        # 判断自增字段数据类型：number、string、time
        if (fld["fldsrc"]).lower() == incrfld.lower():
            incrfld_type = fld["fldout"]
            if curpos == None or curpos == "": 
                if incrfld_type == "number":
                    curpos = "0"
                elif incrfld_type == "string":
                    curpos = " "
                elif incrfld_type == "time":
                    curpos = "1980-01-01 00:00:00"
                else:
                    curpos = " "

    if db != None:
        # 循环读取日志数据
        while True:
            try:
                lst = db.getData( flds, incrfld_type, curpos, curpos_stime, curpos_etime )
                print "Start fetch data(t_database.dbid=" + str(dbid) + "): records=" + str(len(lst)), datetime.datetime.now()

                if lst == None or len( lst ) == 0:
                    # 1 主键增量； 2 时间增量； 0 一次性导入数据
                    if inctype == 2:
                        if incrfld_type == "number":
                            curpos = "0"
                        elif incrfld_type == "string":
                            curpos = " "
                        elif incrfld_type == "time":
                            curpos = "1980-01-01 00:00:00"
                        else:
                            curpos = " "
                        
                        if curpos_stime == '1980-01-01 00:00:00':
                            curpos_stime = curpos_etime
                            curpos_etime = db.getMinTime()
                        else:
                            curpos_stime = curpos_etime
                            curpos_etime = db.getMaxTime()

                            #如果时间差距过大，为防止数据量太大导致排序速度慢，那么让日期逐步增加
                            t1 = datetime.datetime.strptime(curpos_stime, '%Y-%m-%d %H:%M:%S')
                            t2 = datetime.datetime.strptime(curpos_etime, '%Y-%m-%d %H:%M:%S')
                            delta = t2 - t1
                            if delta.days > 2:
                                    curpos_etime = (t1 + datetime.timedelta(1)).strftime('%Y-%m-%d %H:%M:%S')
                    elif inctype == 0:
                        return
                    time.sleep( DELAYTIME )
                else:
                    # 取最后一行数据
                    row = lst[len(lst)-1]
                    # 取最后一行数据的主键值
                    curpos = row[len(flds)]

                    # 更新下次要提取数据的位置
                    postdata.updCurPosition( dbid, curpos, curpos_stime, curpos_etime )
                    # 向ES中写数据
                    es.bulkInsData( lst, inctype, timefld, lstFlds, incrfld, tbname, idxname )

                    #if len(lst) < reclimit:
                    #    time.sleep( DELAYTIME )
                
                # 卡口测试
                if tbname == "b_bk_jgcl":
                    conf = sys_config.SysConfig( configFile )
                    _logFile = conf.getConfig( "dataextract", "logFile" )
                    _instance = conf.getConfig( "dataextract", "instanceName" )
                    sys_log.SysLog( _logFile, _instance ).writeLog( "error",  "=====|" + tbname + "|" + str(time.time()) + "|" + str(curpos) + "|" + str(curpos_stime) + "|" + str(curpos_etime))
            except Exception, e:
                conf = sys_config.SysConfig( configFile )
                _logFile = conf.getConfig( "dataextract", "logFile" )
                _instance = conf.getConfig( "dataextract", "instanceName" )
                sys_log.SysLog( _logFile, _instance ).writeLog( "error", "tbname" + " --- " + str(traceback.format_exc()) )
                

def createTask( listTask ):
    """
    创建并开始新进程
    """

    for dic in listTask:
        # 主键
        dbID = str( dic["dbid"] )
        # 数据库连接名称
        conName = dic["conname"]
        # 表名
        tbName = dic["tbname"]
        # 索引名后缀
        idxName = dic["idxname"]
        if idxName is None or idxName.strip() == "": idxName = tbName
        # 每次读取的最大记录数
        recLimit = dic["reclimit"]
        # 数据增量方式：1 主键增量； 2 时间增量； 0 一次性导入数据
        incType = dic["inctype"]
        # 日期字段，增量方式为2时有用
        timeFld = dic["timefld"]
        # 主键字段
        incrFld = dic["incrfld"]
        # 输出到message字段的数据拼接形式，在廊坊市局版本里无用
        msgFld = dic["msgfld"]
        # 当前已读取记录中自增长字段的最大值
        curPos = dic["curpos"]
        if dic["inctype"] == 2:
            curPosSTime = dic["curpos_stime"]
            curPosETime = dic["curpos_etime"]
        else:
            curPosSTime = None
            curPosETime = None

        argus = ( int(dbID), conName, tbName, idxName, recLimit, incType, timeFld, incrFld, msgFld, curPos, curPosSTime, curPosETime )

        p = multiprocessing.Process( name="Process" + dbID, target=taskFunc, args=argus )

        dicTask['Task'+dbID] = p
        dicTask['Task'+dbID].start()
        #print 'Task %s start time is: %s' % (dbID, datetime.datetime.now())


def deleteTask( listTask ):
    """
    停止并删除进程
    """

    for dic in listTask:
        dbID = str(dic["dbid"])
        if dicTask.has_key('Task'+dbID):
            # 停止计划任务
            dicTask['Task'+dbID].terminate()
            dicTask['Task'+dbID].join()
            # 从字典中移除
            del dicTask['Task'+dbID]


def run(start_type = None):
    global dicTask,  postdata
    
    conf = sys_config.SysConfig(configFile)
    # 进程号文件名
    pidFile = conf.getConfig("dataextract", "pidFile")

    if start_type == None:
        if os.path.exists(pidFile):
            os.remove(pidFile)
        pFile = open(pidFile, "w")
        pFile.write(str(os.getpid()))
        pFile.close()

    # 生成postdata对象
    postdata = postgresdataextract.PostgresData( configFile )

    # 清空表t_dbnew、t_dbupd、t_dbdel
    postdata.clearDataExtraction()

    # 读取活动的数据库数据提取任务   在这个地方把数据提取任务直接传过来
    lstTask = postdata.getDataExtractions()

    if lstTask != None and len( lstTask ) > 0:
        # 创建并启动数据库数据提取任务
        createTask( lstTask )


    while True:
        # 延时
        time.sleep( LOOPTIME )

        # 读取变化过的数据抽取任务
        lstNew = postdata.getNewDataExtraction()
        lstUpd = postdata.getUpdDataExtraction()
        lstDel = postdata.getDelDataExtraction()

        # 清空表t_dbnew、t_dbupd、t_dbdel
        postdata.clearDataExtraction()

        # 创建新的数据抽取任务
        if lstNew != None and len( lstNew ) > 0:
            createTask( lstNew )

        # 修改已存在的数据抽取任务
        if lstUpd != None and len( lstUpd ) > 0:
            # 先停止
            deleteTask( lstUpd )
            # 再重建
            createTask( lstUpd )

        # 删除数据抽取任务
        if lstDel != None and len( lstDel ) > 0:
            deleteTask( lstDel )
        
        # 如果没有数据读取任务则退出
        #if len( dicTask ) == 0:
        #    return


def check_tasklist():
    """
    检测当前任务的执行状态，包括：数据库连接是否正常、数据表主键最大值、已处理的主键值、错误信息、提取的数据表字段
    """

    # 生成postdata对象
    postdata = postgresdataextract.PostgresData( configFile )

    # 读取活动的数据库数据提取任务
    lstTask = postdata.getDataExtractions()

    f = open("/root/check_tasklist.log", "w")
    for task in lstTask:
        # 返回值为None或有以下键的字典( conname, conntype, hostname, port, dbname, username, userpass, usepooling )
        connInfo = postdata.getConnInfo( task["conname"] )
        l = "=========================================================================================="
        f.write(l + "\n\n")
        print l

        l = "########## 基本信息 ##########"
        f.write(l + "\n")
        print l

        l = "数据库配置ID:  " + str(task["dbid"])
        f.write(l + "\n")
        print l

        l = "数据库连接名称:" + task["conname"] + " (" + connInfo["hostname"] + " / " + str(connInfo["port"]) + " / " + connInfo["dbname"] + " / " + connInfo["username"] + " / " + base64.decodestring(connInfo["userpass"]) +")"
        f.write(l + "\n")
        print l

        l = "表名:          " + task["tbname"]
        f.write(l + "\n")
        print l

        l = "索引名:        " + "idx_" + task["idxname"]
        f.write(l + "\n")
        print l

        l = "批量导入记录数:" + str(task["reclimit"])
        f.write(l + "\n")
        print l

        l = "数据增量方式:  " + str(task["inctype"])
        f.write(l + "\n")
        print l

        l = "主键字段:      " + task["incrfld"] + " (当前已处理主键值:" + str(task["curpos"]) + ")"
        f.write(l + "\n")
        print l

        l = "日期字段:      " + task["timefld"] + " (当前已处理日期范围:" + str(task["curpos_stime"]) + " ～ " + str(task["curpos_etime"]) + ")"
        f.write(l + "\n\n")
        print l

        l = "########## 数据库连接测试 ##########"
        f.write(l + "\n")
        print l
        f.flush()
        
        dbDic = {}
        dbDic['dbhost'] = connInfo["hostname"]
        dbDic['dbport'] = str(connInfo["port"])
        dbDic['dbname'] = connInfo["dbname"]
        dbDic['dbuser'] = connInfo["username"]
        dbDic['dbpass'] = base64.decodestring(connInfo["userpass"])

        dbDic['tbname'] = task["tbname"]
        dbDic['reclimit'] = str( task["reclimit"] )
        dbDic['inctype'] = task["inctype"]
        dbDic['timefld'] = task["timefld"]
        dbDic['incrfld'] = task["incrfld"]
        dbDic['msgfld'] = task["msgfld"]

        dbfac = dbfactory.DbFactory( dbDic )
        db = dbfac.factory( connInfo["conntype"] )
        try:
            l = "数据表结构(" + task["tbname"] + "):"
            f.write(l + "\n")
            print l
            
            lstFlds = db.getTableStructure()
            for fld in lstFlds:
                l = "  " + fld["fldsrc"].ljust(30, ' ') + "    " + fld["fldout"]
                f.write(l + "\n")
                print l
            f.write(l + "\n")
            f.flush()
        except Exception, e:
            l = "错误信息:"
            f.write(l + "\n")
            print l

            l = "******************************************************************************************"
            f.write(l + "\n")
            print l

            l = str(traceback.format_exc())
            f.write(l + "\n")
            print l

            l = "******************************************************************************************"
            f.write(l + "\n")
            print l
        
    f.close()
        


#测试用
if __name__ == "__main__":
    
    #run()
    check_tasklist()
