#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-08-30"

import os
import traceback

from utils import sys_config, sys_log, db_manager


# 配置文件名称
CONFIG_FILE = "mdstack.conf"

# 信息字段名称
FIELDMSG = "msg.message"
#FIELDMSG = "@message"

class PostgresData():
    """
    postgres数据处理类
    """

    def __init__( self, cfgFile = None ):
        if cfgFile == None:
            #cfgFile = sys_config.getDir() + "/conf/" + CONFIG_FILE
            cfgFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/" + CONFIG_FILE
            if os.path.exists(cfgFile) == False:
                cfgFile = "/opt/mdstack/conf/mdstackd/" + CONFIG_FILE

        conf = sys_config.SysConfig( cfgFile )
        
        # 数据库连接相关
        self._dbhost = conf.getConfig( "postgresql", "dbHost" )
        self._dbport = conf.getConfig( "postgresql", "dbPort" )
        self._dbname = conf.getConfig( "postgresql", "dbName" )
        self._dbuser = conf.getConfig( "postgresql", "dbUser" )
        self._dbpwd = conf.getConfig( "postgresql", "dbPwd" )

        # 日志文件
        self._logFile = conf.getConfig( "tasksched", "logFile" )

        # 实例名
        self._instance = conf.getConfig( "tasksched", "instanceName" )

    def _getCursor( self ):
        """
        获取postgresql数据库的游标
        """

        cursor = None
        try:
            cursor = db_manager.PostgreDBManager( self._dbuser, self._dbpwd, \
                    self._dbhost, self._dbport, self._dbname ).getCursor()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )

        return cursor

    def clearSchedule( self ):
        """
        清空表t_schednew、t_schedupd、t_scheddel
        """
        
        cur = self._getCursor()
        if cur == None:
            return

        try:
            cur.execute( "truncate table t_schednew;" )
            cur.execute( "truncate table t_schedupd;" )
            cur.execute( "truncate table t_scheddel;" )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()


    def getTask( self ):
        """
        从表t_schedule中读取活动的计划任务列表(schedstatus=0)
        """

        listTask = None
        cur = self._getCursor()
        if cur == None:
            return listTask

        try:
            sql = "select schedid, searchcond, searchstart, searchend, schedstart, schedend,"
            sql += " schedtime, schedcron, warnornot, warncondop, warncondval, warnlevel, saveresult"
            sql += " from t_schedule where schedstatus = 0 order by schedid;"

            cur.execute( sql )
            listTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listTask

    def getNewTask( self ):
        """
        从表t_schedule、t_schednew中读取新建计划任务
        """

        listNewTask = None
        cur = self._getCursor()
        if cur == None:
            return listNewTask

        try:
            sql = "select schedid, searchcond, searchstart, searchend, schedstart, schedend,"
            sql += " schedtime, schedcron, warnornot, warncondop, warncondval, warnlevel, saveresult"
            sql += " from t_schedule where schedstatus = 0"
            sql += " and schedid in (select schedid from t_schednew) order by schedid;"

            cur.execute( sql )
            listNewTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listNewTask

    def getUpdTask( self ):
        """
        从表t_schedule、t_schedupd中读取修改的计划任务
        """

        listUpdTask = None
        cur = self._getCursor()
        if cur == None:
            return listUpdTask

        try:
            sql = "select schedid, searchcond, searchstart, searchend, schedstart, schedend,"
            sql += " schedtime, schedcron, warnornot, warncondop, warncondval, warnlevel, saveresult"
            sql += " from t_schedule where schedstatus = 0"
            sql += " and schedid in (select distinct schedid from t_schedupd) order by schedid;"

            cur.execute( sql )
            listUpdTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listUpdTask

    def getDelTask( self ):
        """
        从表t_scheddel中读取删除的计划任务
        """

        listDelTask = None
        cur = self._getCursor()
        if cur == None:
            return listDelTask

        try:
            sql = "select schedid from t_scheddel;"

            cur.execute( sql )
            listDelTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listDelTask

    def expireschedule( self, schedid ):
        """
        将计划任务设置为到期
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            sql = "update t_schedule set schedstatus = 2 where schedid = %s;"
            
            cur.execute( sql, (schedid,) )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

    def getGroupList( self ):
        """
        获取组名列表
        """

        listGroup = None
        cur = self._getCursor()
        if cur == None:
            return listGroup

        try:
            sql = "select groupname from t_confgroup;"

            cur.execute( sql )
            listGroup = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listGroup

    def newWarn( self, schedID, schedExecTime, warnLevel, searchCond, searchStart, searchEnd, resultCnt ):
        """
        向t_warn中插入一条数据，并返回warnid，没插入成功返回0
        """

        wid = 0
        cur = self._getCursor()
        if cur == None:
            return wid

        try:
            sql = "INSERT INTO t_warn(schedid, schedexectime, warnlevel, searchcond, searchstart, searchend, resultcnt) "
            sql += "VALUES(%s, %s, %s, %s, %s, %s, %s)  RETURNING warnid;"

            cur.execute( sql, (schedID, schedExecTime, warnLevel, searchCond, searchStart, searchEnd, resultCnt) )
            itemWarn = cur.fetchone()
            wid = int( itemWarn['warnid'] )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return wid

    def newSchedResult( self, schedID, schedExecTime, searchCond, searchStart, searchEnd, resultCnt, savedCnt ):
        """
        向t_schedresult中插入一条数据，并返回resultid，没插入成功返回0
        """

        rid = 0
        cur = self._getCursor()
        if cur == None:
            return rid

        try:
            sql = "INSERT INTO t_schedresult(schedid, schedexectime, searchcond, searchstart, searchend, resultcnt, savedcnt) "
            sql += "VALUES(%s, %s, %s, %s, %s, %s, %s)  RETURNING resultid;"

            cur.execute( sql, (schedID, schedExecTime, searchCond, searchStart, searchEnd, resultCnt, savedCnt) )
            itemSchedResult = cur.fetchone()
            rid = int( itemSchedResult['resultid'] )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return rid

    def newResultDetail( self, warnID, resultID, lstDetail ):
        """
        向t_resultdetail中插入数据
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            sql = "INSERT INTO t_resultdetail(warnid, resultid, idxname, typename, msgid, message) "
            sql += "VALUES(%s, %s, %s, %s, %s, %s);"

            for rst in lstDetail:
                idxName = rst["_index"]
                typeName = rst["_type"]
                msgID = rst["_id"]
                #msg = rst["fields"][FIELDMSG]
                msg = rst["_source"][FIELDMSG]
                
                cur.execute( sql, (warnID, resultID, idxName, typeName, msgID, msg) )

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

    def is_master(self):
        """
        判断是否主服务器：
        如果未启用集群，则返回True；
        如果启用集群，是主服务器，返回True，不是主服务，返回False；
        """

        bis = True
        cur = self._getCursor()
        if cur == None:
            return bis

        sql = "select enablecluster, ismaster from t_cluster limit 1;"
        try:
            cur.execute(sql)
            row = cur.fetchone()
            if row != None:
                if row["enablecluster"] == "Y" and row["ismaster"] == "N":
                    bis = False
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return bis