#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-09-16"

import traceback
import os
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
        self._logFile = conf.getConfig( "dataextract", "logFile" )

        # 实例名
        self._instance = conf.getConfig( "dataextract", "instanceName" )

    def _getCursor( self):
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

    def clearDataExtraction( self ):
        """
        清空表t_dbnew、t_dbupd、t_dbdel
        """
        
        cur = self._getCursor()
        if cur == None:
            return

        try:
            cur.execute( "truncate table t_dbnew;" )
            cur.execute( "truncate table t_dbupd;" )
            cur.execute( "truncate table t_dbdel;" )
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

    def getDataExtractions( self ):
        """
        从表t_database中读取数据库数据提取任务列表(dbstatus=0)
        """

        listTask = None
        cur = self._getCursor()
        if cur == None:
            return listTask

        try:
            sql = "select dbid, groupid, compname, sysname, conname, tbname, idxname, reclimit, inctype, timefld, incrfld, msgfld, curpos,"
            sql += " case when curpos_stime is null then '1980-01-01 00:00:00' else to_char(curpos_stime, 'yyyy-mm-dd hh24:mi:ss') end curpos_stime,"
            sql += " case when curpos_etime is null then '1980-01-01 00:00:00' else to_char(curpos_etime, 'yyyy-mm-dd hh24:mi:ss') end curpos_etime"
            sql += " from t_database where dbstatus = 0 order by dbid;"

            cur.execute( sql )
            listTask = cur.fetchall()

        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listTask

    def getNewDataExtraction( self ):
        """
        从表t_database、t_dbnew中读取新建数据库数据提取任务
        """

        listNewTask = None
        cur = self._getCursor()
        if cur == None:
            return listNewTask

        try:
            sql = "select dbid, groupid, compname, sysname, conname, tbname, idxname, reclimit, inctype, timefld, incrfld, msgfld, curpos,"
            sql += " case when curpos_stime is null then '1980-01-01 00:00:00' else to_char(curpos_stime, 'yyyy-mm-dd hh24:mi:ss') end curpos_stime,"
            sql += " case when curpos_etime is null then '1980-01-01 00:00:00' else to_char(curpos_etime, 'yyyy-mm-dd hh24:mi:ss') end curpos_etime"
            sql += " from t_database where dbstatus = 0"
            sql += " and dbid in (select dbid from t_dbnew) order by dbid;"

            cur.execute( sql )
            listNewTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listNewTask

    def getUpdDataExtraction( self ):
        """
        从表t_database、t_dbupd中读取修改的数据库数据提取任务
        """

        listUpdTask = None
        cur = self._getCursor()
        if cur == None:
            return listUpdTask

        try:
            sql = "select dbid, groupid, compname, sysname, conname, tbname, idxname, reclimit, inctype, timefld, incrfld, msgfld, curpos,"
            sql += " case when curpos_stime is null then '1980-01-01 00:00:00' else to_char(curpos_stime, 'yyyy-mm-dd hh24:mi:ss') end curpos_stime,"
            sql += " case when curpos_etime is null then '1980-01-01 00:00:00' else to_char(curpos_etime, 'yyyy-mm-dd hh24:mi:ss') end curpos_etime"
            sql += " from t_database where dbstatus = 0"
            sql += " and dbid in (select distinct dbid from t_dbupd) order by dbid;"

            cur.execute( sql )
            listUpdTask = cur.fetchall()

        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listUpdTask

    def getDelDataExtraction( self ):
        """
        从表t_dbdel中读取删除的数据库数据提取任务
        """

        listDelTask = None
        cur = self._getCursor()
        if cur == None:
            return listDelTask

        try:
            sql = "select dbid from t_dbdel;"

            cur.execute( sql )
            listDelTask = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listDelTask

    def updCurPosition( self, dbid, curpos, curpos_stime, curpos_etime ):
        """
        设置已提取数据的位置
        """

        cur = self._getCursor()
        if cur == None:
            return

        try:
            sql = "update t_database set curpos = %s, curpos_stime = %s, curpos_etime = %s where dbid = %s;"
            if curpos_stime is None:
                cur.execute(sql, (str(curpos), None, None, dbid))
            else:
                cur.execute(sql, (str(curpos), curpos_stime, curpos_etime, dbid))
            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
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

    def getConnInfo( self, conname ):
        """
        获取数据库连接信息
        """

        info = None
        cur = self._getCursor()
        if cur == None:
            return info

        try:
            sql = "select conname, conntype, hostname, port, dbname, username, userpass, usepooling"
            sql += " from t_connection where conname = %s;"

            cur.execute( sql, (conname, ) )
            info = cur.fetchone()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return info

    def getDbFieldList( self, dbid ):
        """
        获取从数据库提取数据的字段列表
        """

        listField = None
        cur = self._getCursor()
        if cur == None:
            return listField

        try:
            sql = "select fldid, fldsrc, fldout from t_dbfields where dbid = %s order by fldid;"

            cur.execute( sql, (dbid, ) )
            listField = cur.fetchall()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()

        return listField

    def saveDbFieldList(self, dbid, lstFlds):
        """
        存储字段列表
        """

        cur = self._getCursor()

        try:
            sql = "select * from t_dbfields where dbid = %s"
            cur.execute(sql, (dbid, ))
            rs = cur.fetchall()

            # 只导入一次（视情况修改）
            if len(rs) == 0:
                for fld in lstFlds:
                    sql = "insert into t_dbfields(dbid, fldsrc, fldout) values(%s, %s, %s)"
                    cur.execute(sql, (dbid, fld["fldsrc"], fld["fldout"]))
                cur.connection.commit()
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
        finally:
            cur.close()
            cur.connection.close()