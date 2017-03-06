#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-08-30"

import os
import traceback
from utils import sys_config, sys_log, db_manager


# 配置文件名称
CONFIG_FILE = "mdstack.conf"

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
        self._logFile = conf.getConfig( "pullfile", "logFile" )

        # 实例名
        self._instance = conf.getConfig( "pullfile", "instanceName" )

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
        清空表t_pfnew、t_pfupd、t_pfdel
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            cur.execute( "truncate table t_pfnew;" )
            cur.execute( "truncate table t_pfupd;" )
            cur.execute( "truncate table t_pfdel;" )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

    def getTask( self ):
        """
        从表t_pullfile中读取活动的文件提取任务列表(pfstatus=0)
        """

        listTask = None
        cur = self._getCursor()
        if cur == None:
            return listTask

        try:
            sql = "select pfid, groupid, logsource, configpath, protocol, port, username, userpass,"
            sql += " fpath, files, onetime, schedstart, schedend, schedtime, schedcron"
            sql += " from t_pullfile where pfstatus = 0 order by pfid;"

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
        从表t_pullfile、t_pfnew中读取新建文件提取任务
        """

        listNewTask = None
        cur = self._getCursor()
        if cur == None:
            return listNewTask

        try:
            sql = "select pfid, groupid, logsource, configpath, protocol, port, username, userpass,"
            sql += " fpath, files, onetime, schedstart, schedend, schedtime, schedcron"
            sql += " from t_pullfile where pfstatus = 0"
            sql += " and pfid in (select pfid from t_pfnew) order by pfid;"

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
        从表t_pullfile、t_pfupd中读取修改的文件提取任务
        """

        listUpdTask = None
        cur = self._getCursor()
        if cur == None:
            return listUpdTask

        try:
            sql = "select pfid, groupid, logsource, configpath, protocol, port, username, userpass,"
            sql += " fpath, files, onetime, schedstart, schedend, schedtime, schedcron"
            sql += " from t_pullfile where pfstatus = 0"
            sql += " and pfid in (select distinct pfid from t_pfupd) order by pfid;"

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
        从表t_pfdel中读取删除的文件提取任务
        """

        listDelTask = None
        cur = self._getCursor()
        if cur == None:
            return listDelTask

        try:
            sql = "select pfid from t_pfdel;"

            cur.execute( sql )
            listDelTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listDelTask

    def setschedulestatus( self, pfid, status ):
        """
        设置提取文件任务的状态 
        status: 1 完成 2 失效 到期
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            sql = "update t_pullfile set pfstatus = %s, lastmoditime = now() where pfid = %s;"

            cur.execute( sql, (status, pfid) )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

    def insFileHash( self, hashvalue, filename, lastmoditime, configpath, logsource ):
        """
        向表t_filehash中插入一条记录
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            sql = "insert into t_pullfilehash( hashvalue, filename, lastmoditime, configpath, logsource ) "
            sql = sql + "values( %s, %s, %s, %s, %s );"

            cur.execute( sql, (hashvalue, filename, lastmoditime, configpath, logsource) )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()


    def existsFileHash( self, hashvalue ):
        """
        通过表t_pullfilefilehash中的Hash值判断文件是否被处理过
        """

        exists = False 
        cur = self._getCursor()
        if cur == None:
            return exists

        try:
            sql = "select hashvalue from  t_pullfilehash where hashvalue = %s;"

            cur.execute( sql, (hashvalue, ) )
            listHash = cur.fetchall()

            if listHash != None and len( listHash ) > 0:
                exists = True
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return exists

    def getGroupName( self, groupid ):
        """
        获取组名
        """

        groupName = groupid
        cur = self._getCursor()
        if cur == None:
            return groupName

        try:
            sql = "select groupname from t_confgroup where groupid = %s;"

            cur.execute( sql, (groupid,) )
            row = cur.fetchone()
            groupName = row["groupname"]
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return groupName

