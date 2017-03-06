#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-10-23"

import os
import datetime
import traceback

from utils import sys_config, sys_log, db_manager


# 配置文件名称
CONFIG_FILE = "mdstack.conf"

class PostgresData():
    """
    postgres数据处理类
    """

    def __init__(self, cfgFile = None):
        if cfgFile == None:
            #cfgFile = sys_config.getDir() + "/conf/" + CONFIG_FILE
            cfgFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/" + CONFIG_FILE
            if os.path.exists(cfgFile) == False:
                cfgFile = '/opt/mdstack/conf/mdstackd/' + CONFIG_FILE

        conf = sys_config.SysConfig(cfgFile)

        # 数据库连接相关
        self._dbhost = conf.getConfig("postgresql", "dbHost")
        self._dbport = conf.getConfig("postgresql", "dbPort")
        self._dbname = conf.getConfig("postgresql", "dbName")
        self._dbuser = conf.getConfig("postgresql", "dbUser")
        self._dbpwd = conf.getConfig("postgresql", "dbPwd")

        # 日志文件
        self._logFile = conf.getConfig("datastats", "logFile")

        # 实例名
        self._instance = conf.getConfig("datastats", "instanceName")

    def _getCursor(self):
        """
        获取postgresql数据库的游标
        """

        cursor = None
        try:
            cursor = db_manager.PostgreDBManager(self._dbuser, self._dbpwd, \
                    self._dbhost, self._dbport, self._dbname).getCursor()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))

        return cursor

    def new_stats_day(self, statsDate, logsCnt, logsDatasize, flowsCnt, flowsDatasize, dataSize):
        """
        向表t_stats_day中插入一条数据;
        如果statsDate日期的数据已存在,则修改
        """
        
        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_stats_day(statsdate, logscnt, logsdatasize, flowscnt, flowsdatasize, datasize) values(%s, %s, %s, %s, %s, %s);"
        upd = "update t_stats_day set logscnt = %s, logsdatasize = %s, flowscnt = %s, flowsdatasize = %s, datasize = %s where statsdate = %s;"
        sel = "select logscnt, logsdatasize, flowscnt, flowsdatasize, datasize from t_stats_day where statsdate = %s;"

        try:
            cur.execute(sel, (statsDate, ))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (logsCnt, logsDatasize, flowsCnt, flowsDatasize, dataSize, statsDate))
            else:
                cur.execute(ins, (statsDate, logsCnt, logsDatasize, flowsCnt, flowsDatasize, dataSize))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_stats_hour(self, statsDate, hourNum, logsCnt, flowsCnt, flowsBytes, flowsPackets):
        """
        向表t_stats_hour中插入一条数据;
        如果statsDate日期对应小时的数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_stats_hour(statsdate, hournum, logscnt, flowscnt, flowsbytes, flowspackets) values(%s, %s, %s, %s, %s, %s);"
        upd = "update t_stats_hour set logscnt = %s, flowscnt = %s, flowsbytes = %s, flowspackets = %s where statsdate = %s and hournum = %s;"
        sel = "select logscnt, flowscnt, flowsbytes, flowspackets from t_stats_hour where statsdate = %s and hournum = %s;"

        try:
            cur.execute(sel, (statsDate, hourNum))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (logsCnt, flowsCnt, flowsBytes, flowsPackets, statsDate, hourNum))
            else:
                cur.execute(ins, (statsDate, hourNum, logsCnt, flowsCnt, flowsBytes, flowsPackets))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbydn_day(self, dn, statsDate, cnt):
        """
        向表t_statsbydn_day中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbydn_day(dn, statsdate, statscnt) values(%s, %s, %s);"
        upd = "update t_statsbydn_day set statscnt = %s where dn = %s and statsdate = %s;"
        sel = "select statscnt from t_statsbydn_day where dn = %s and statsdate = %s;"

        try:
            cur.execute(sel, (dn, statsDate))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, dn, statsDate))
            else:
                cur.execute(ins, (dn, statsDate, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbydn_hour(self, dn, statsDate, hourNum, cnt):
        """
        向表t_statsbydn_hour中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbydn_hour(dn, statsdate, hournum, statscnt) values(%s, %s, %s, %s);"
        upd = "update t_statsbydn_hour set statscnt = %s where dn = %s and statsdate = %s and hournum = %s;"
        sel = "select statscnt from t_statsbydn_hour where dn = %s and statsdate = %s and hournum = %s;"

        try:
            cur.execute(sel, (dn, statsDate, hourNum))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, dn, statsDate, hourNum))
            else:
                cur.execute(ins, (dn, statsDate, hourNum, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbygroup_day(self, groupName, statsDate, cnt):
        """
        向表t_statsbygroup_day中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbygroup_day(groupname, statsdate, statscnt) values(%s, %s, %s);"
        upd = "update t_statsbygroup_day set statscnt = %s where groupname = %s and statsdate = %s;"
        sel = "select statscnt from t_statsbygroup_day where groupname = %s and statsdate = %s;"

        try:
            cur.execute(sel, (groupName, statsDate))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, groupName, statsDate))
            else:
                cur.execute(ins, (groupName, statsDate, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbygroup_hour(self, groupName, statsDate, hourNum, cnt):
        """
        向表t_statsbygroup_hour中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbygroup_hour(groupname, statsdate, hournum, statscnt) values(%s, %s, %s, %s);"
        upd = "update t_statsbygroup_hour set statscnt = %s where groupname = %s and statsdate = %s and hournum = %s;"
        sel = "select statscnt from t_statsbygroup_hour where groupname = %s and statsdate = %s and hournum = %s;"

        try:
            cur.execute(sel, (groupName, statsDate, hourNum))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, groupName, statsDate, hourNum))
            else:
                cur.execute(ins, (groupName, statsDate, hourNum, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbyhost_day(self, host, statsDate, cnt):
        """
        向表t_statsbyhost_day中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbyhost_day(host, statsdate, statscnt) values(%s, %s, %s);"
        upd = "update t_statsbyhost_day set statscnt = %s where host = %s and statsdate = %s;"
        sel = "select statscnt from t_statsbyhost_day where host = %s and statsdate = %s;"

        try:
            cur.execute(sel, (host, statsDate))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, host, statsDate))
            else:
                cur.execute(ins, (host, statsDate, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbyhost_hour(self, host, statsDate, hourNum, cnt):
        """
        向表t_statsbyhost_hour中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbyhost_hour(host, statsdate, hournum, statscnt) values(%s, %s, %s, %s);"
        upd = "update t_statsbyhost_hour set statscnt = %s where host = %s and statsdate = %s and hournum = %s;"
        sel = "select statscnt from t_statsbyhost_hour where host = %s and statsdate = %s and hournum = %s;"

        try:
            cur.execute(sel, (host, statsDate, hourNum))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (cnt, host, statsDate, hourNum))
            else:
                cur.execute(ins, (host, statsDate, hourNum, cnt))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbynode_day(self, nodeName, statsDate, flowsCnt, flowsBytes, flowsPackets):
        """
        向表t_statsbynode_day中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbynode_day(nodename, statsdate, flowscnt, flowsbytes, flowspackets) values(%s, %s, %s, %s, %s);"
        upd = "update t_statsbynode_day set flowscnt = %s, flowsbytes = %s, flowspackets = %s where nodename = %s and statsdate = %s;"
        sel = "select flowscnt, flowsbytes, flowspackets from t_statsbynode_day where nodename = %s and statsdate = %s;"

        try:
            cur.execute(sel, (nodeName, statsDate))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (flowsCnt, flowsBytes, flowsPackets, nodeName, statsDate))
            else:
                cur.execute(ins, (nodeName, statsDate, flowsCnt, flowsBytes, flowsPackets))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def new_statsbynode_hour(self, nodeName, statsDate, hourNum, flowsCnt, flowsBytes, flowsPackets):
        """
        向表t_statsbynode_hour中插入一条数据;
        如果数据已存在,则修改
        """

        cur = self._getCursor()
        if cur == None:
            return

        ins = "insert into t_statsbynode_hour(nodename, statsdate, hournum, flowscnt, flowsbytes, flowspackets) values(%s, %s, %s, %s, %s, %s);"
        upd = "update t_statsbynode_hour set flowscnt = %s, flowsbytes = %s, flowspackets = %s where nodename = %s and statsdate = %s and hournum = %s;"
        sel = "select flowscnt, flowsbytes, flowspackets from t_statsbynode_hour where nodename = %s and statsdate = %s and hournum = %s;"

        try:
            cur.execute(sel, (nodeName, statsDate, hourNum))
            rst = cur.fetchall()
            if len(rst) > 0:
                cur.execute(upd, (flowsCnt, flowsBytes, flowsPackets, nodeName, statsDate, hourNum))
            else:
                cur.execute(ins, (nodeName, statsDate, hourNum, flowsCnt, flowsBytes, flowsPackets))

            cur.connection.commit()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return

    def get_host_list(self):
        """
        获取所有设置中的主机列表
        """
        
        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        sql = "select distinct(host) as host from ("
        sql = sql + "select distinct(logsource) as host from t_syslog union "
        sql = sql + "select distinct(logsource) as host from t_pullfile union "
        sql = sql + "select distinct(hostname) as host from t_connection "
        sql = sql + "where conname in (select conname from t_database)) as t_host;"

        try:
            cur.execute(sql)
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_host_list_day(self, cDate):
        """
        获取所有设置中的主机列表, 包括上传文件的客户机
        """

        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        nextDate = cDate + datetime.timedelta(days = 1)

        sql = "select distinct(host) as host from ("
        sql = sql + "select distinct(logsource) as host from t_syslog union "
        sql = sql + "select distinct(logsource) as host from t_pullfile union "
        sql = sql + "select distinct(hostname) as host from t_connection "
        sql = sql + "where conname in (select conname from t_database) union " 
        sql = sql + "select distinct(logsource) as host from t_filehash "
        sql = sql + "where firstime >= %s and firstime < %s) as t_host;"

        try:
            cur.execute(sql, (cDate, nextDate))
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_group_list(self):
        """
        获取所有设置中的组名列表
        """

        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        sql = "select distinct(groupname) as groupname from t_confgroup "
        sql = sql + "where groupid in ("
        sql = sql + "select distinct(groupid) as groupid from t_syslog union "
        sql = sql + "select distinct(groupid) as groupid from t_pullfile union "
        sql = sql + "select distinct(groupid) as groupid from t_database);"

        try:
            cur.execute(sql)
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_group_list_day(self, cDate):
        """
        获取所有设置中的组名列表, 包括上传文件的组upload
        """

        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        nextDate = cDate + datetime.timedelta(days = 1)

        sql = "select distinct(groupname) as groupname from t_confgroup "
        sql = sql + "where groupid in ("
        sql = sql + "select distinct(groupid) as groupid from t_syslog union "
        sql = sql + "select distinct(groupid) as groupid from t_pullfile union "
        sql = sql + "select distinct(groupid) as groupid from t_database) union "
        sql = sql + "select 'upload' as groupname where exists (select logsource "
        sql = sql + "from t_filehash where firstime >= %s and firstime < %s limit 1);"

        try:
            cur.execute(sql, (cDate, nextDate))
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_dn_list(self):
        """
        获取所有设置中dn列表
        """

        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        sql = "select distinct(dn) as dn from ("
        sql = sql + "select distinct(configpath) as dn from t_syslog union "
        sql = sql + "select distinct(configpath) as dn from t_pullfile union "
        sql = sql + "select distinct('biz:' || COALESCE(compname,'') || ':' || COALESCE(sysname,'')) "
        sql = sql + "as dn from t_database) as t_dn;"

        try:
            cur.execute(sql)
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_dn_list_day(self, cDate):
        """
        获取所有设置中dn列表, 包括上传文件的
        """

        lst=[]
        cur = self._getCursor()
        if cur == None:
            return lst

        nextDate = cDate + datetime.timedelta(days = 1)

        sql = "select distinct(dn) as dn from ("
        sql = sql + "select distinct(configpath) as dn from t_syslog union "
        sql = sql + "select distinct(configpath) as dn from t_pullfile union "
        sql = sql + "select distinct('biz:' || COALESCE(compname,'') || ':' || COALESCE(sysname,'')) "
        sql = sql + "as dn from t_database union "
        sql = sql + "select distinct(configpath) as dn from t_filehash "
        sql = sql + "where firstime >= %s and firstime < %s) as t_dn;"

        try:
            cur.execute(sql, (cDate, nextDate))
            lst = cur.fetchall()
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return lst

    def get_retain(self, idxType = "logs"):
        """
        获取索引数据保留期限
        """

        ret = 0
        cur = self._getCursor()
        if cur == None:
            return ret

        sql = "select cast(svalue as integer) retention from t_settings where skey = '"
        sql = sql + idxType + "_retain' limit 1;"
        try:
            cur.execute(sql)
            lst = cur.fetchone()
            if lst != None:
                ret = lst["retention"]
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return ret

    def is_stats_data(self, cDate, hourNum = None):
        """
        判断该时间是否有统计数据
        """
        
        bis = True
        cur = self._getCursor()
        if cur == None:
            return bis
        
        if hourNum == None:
            sql = "select logscnt, flowscnt from t_stats_day where statsdate = %s;"
            args = (cDate, )
        else:
            sql = "select logscnt, flowscnt from t_stats_hour where statsdate = %s and hournum = %s;"
            args = (cDate, hourNum)

        try:
            cur.execute(sql, args)
            lst = cur.fetchall()

            bis = (len(lst) > 0)
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))
        finally:
            cur.close()
            cur.connection.close()

        return bis

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

# 测试用
if __name__ == "__main__":
    postdata = PostgresData()
    
    import datetime
    sdate = datetime.date(2013, 10, 23)

    """
    postdata.new_stats_day(sdate, 10)

    postdata.new_stats_hour(sdate, 1, 10)
    
    postdata.new_statsbydn_day("hw:fw:leadsec:2.0", sdate, 10)

    postdata.new_statsbydn_hour("hw:fw:leadsec:2.0", sdate, 1, 10)

    postdata.new_statsbygroup_day("test", sdate, 10)

    postdata.new_statsbygroup_hour("test", sdate, 1, 10)

    postdata.new_statsbyhost_day("192.168.2.110", sdate, 10)

    postdata.new_statsbyhost_hour("192.168.2.110", sdate, 1, 10)

    
    postdata.new_stats_day(sdate, 20)

    postdata.new_stats_hour(sdate, 1, 20)

    postdata.new_statsbydn_day("hw:fw:leadsec:2.0", sdate, 20)

    postdata.new_statsbydn_hour("hw:fw:leadsec:2.0", sdate, 1, 20)

    postdata.new_statsbygroup_day("test", sdate, 20)

    postdata.new_statsbygroup_hour("test", sdate, 1, 20)

    postdata.new_statsbyhost_day("192.168.2.110", sdate, 20)

    postdata.new_statsbyhost_hour("192.168.2.110", sdate, 1, 20)
    """

    lst = postdata.get_host_list()
    print lst

    lst = postdata.get_group_list()
    print lst

    lst = postdata.get_dn_list()
    print lst

    print postdata.is_stats_data(sdate)
    print postdata.is_stats_data(sdate, 1)

    print postdata.get_retain()

