#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-10-23"

import os
import time
import traceback
import datetime

from apscheduler.scheduler import Scheduler

import estats
import postgrestats

from utils import sys_config, sys_log
from custom_dashboard import stats_dashboard
from storagemon import chkdevice

# 配置文件
#configFile = sys_config.getDir() + "/conf/mdstack.conf"
configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
if os.path.exists(configFile) == False:
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
# postgres数据处理对象
postdata = None

# 每日中午12:00重新统计前一日数量，包括每小时和一整天的
# (Year, Month, Week, DayofWeek, Day, Hour, Minute, Second)
cron_noon = (None, None, None, None, None, 12, None, None)

# 每小时后的第5分钟统计上一小时的数量
cron_hour = (None, None, None, None, None, None, 5, None)

# 每日午夜后的第10分钟统计前一日的数量
cron_midnight = (None, None, None, None, None, 0, 10, None)

# 每日3点执行自定义dashboard的统计
cron_dashboard = (None, None, None, None, None, 3, 0, None)

# 定义字段名
FIELD_HOST = "msg.host"
FIELD_DN = "msg.dn"
FIELD_GROUP = "msg.group"
FIELD_NODE = "no"
FIELD_BYTES = "by"
FIELD_PACKETS = "pa"

def stats(cDate):
    """
    统计总数量
    """

    es = estats.ESData(configFile)
    
    # 优化索引
    es.Optimize(cDate, "logs")
    es.Optimize(cDate, "flows")
    
    # 统计总数据量
    stats_day(es, cDate)

    # 统计分小时的总数据量
    for i in range(24):
        stats_hour(es, cDate, i)

def stats_day(es, cDate):
    sTime = datetime.time(hour=0, minute=0, second=0)
    #eTime = datetime.time(hour=23, minute=59, second=59)
    sDatetime = datetime.datetime.combine(cDate, sTime)
    #eDatetime = datetime.datetime.combine(eDate, sTime)
    eDatetime = sDatetime + datetime.timedelta(days = 1)
    
    logscnt = es.Count(sDatetime, eDatetime, idxType = "logs")
    logsdatasize = es.SizeIndex(cDate, idxType = "logs")
    flowscnt = es.Count(sDatetime, eDatetime, idxType = "flows")
    flowsdatasize = es.SizeIndex(cDate, idxType = "flows")
    datasize = logsdatasize + flowsdatasize
    postdata.new_stats_day(cDate, logscnt, logsdatasize, flowscnt, flowsdatasize, datasize)

def stats_hour(es, cDate, hourNum):
    sTime = datetime.time(hour=hourNum, minute=0, second=0)
    #eTime = datetime.time(hour=hourNum, minute=59, second=59)
    sDatetime = datetime.datetime.combine(cDate, sTime)
    #eDatetime = datetime.datetime.combine(cDate, eTime)
    eDatetime = sDatetime + datetime.timedelta(hours = 1)

    logscnt = es.Count(sDatetime, eDatetime, idxType = "logs")
    flowscnt = es.Count(sDatetime, eDatetime, idxType = "flows")
    flowsbytes = es.Sum(sDatetime, eDatetime, FIELD_BYTES, idxType = "flows")
    flowspackets = es.Sum(sDatetime, eDatetime, FIELD_PACKETS, idxType = "flows")
    postdata.new_stats_hour(cDate, hourNum + 1, logscnt, flowscnt, flowsbytes, flowspackets)


def statsbyhost(cDate):
    """
    按主机(HOST)统计数据量
    """

    es = estats.ESData(configFile)
    if es.Exists(cDate):     # 索引是否存在
        #lst = postdata.get_host_list()
        lst = postdata.get_host_list_day(cDate)
        # 按主机统计总数据量
        statsbyhost_day(es, lst, cDate)

        # 按主机统计分小时的总数据量
        for i in range(24):
            statsbyhost_hour(es, lst, cDate, i)

def statsbyhost_day(es, lst, cDate):
    for dic in lst:
        cond = FIELD_HOST + ":\"" + dic["host"] + "\""

        sTime = datetime.time(hour=0, minute=0, second=0)
        #eTime = datetime.time(hour=23, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(days = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbyhost_day(dic["host"], cDate, cnt)

def statsbyhost_hour(es, lst, cDate, hourNum):
    for dic in lst:
        cond = FIELD_HOST + ":\"" + dic["host"] + "\""

        sTime = datetime.time(hour=hourNum, minute=0, second=0)
        #eTime = datetime.time(hour=hourNum, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(hours = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbyhost_hour(dic["host"], cDate, hourNum + 1, cnt)


def statsbygroup(cDate):
    """
    按分组统计数据量
    """

    es = estats.ESData(configFile)
    if es.Exists(cDate):     # 索引是否存在
        #lst = postdata.get_group_list()
        lst = postdata.get_group_list_day(cDate)
        # 按主机统计总数据量
        statsbygroup_day(es, lst, cDate)

        # 按主机统计分小时的总数据量
        for i in range(24):
            statsbygroup_hour(es, lst, cDate, i)

def statsbygroup_day(es, lst, cDate):
    for dic in lst:
        cond = FIELD_GROUP + ":\"" + dic["groupname"] + "\""

        sTime = datetime.time(hour=0, minute=0, second=0)
        #eTime = datetime.time(hour=23, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(days = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbygroup_day(dic["groupname"], cDate, cnt)

def statsbygroup_hour(es, lst, cDate, hourNum):
    for dic in lst:
        cond = FIELD_GROUP + ":\"" + dic["groupname"] + "\""

        sTime = datetime.time(hour=hourNum, minute=0, second=0)
        #eTime = datetime.time(hour=hourNum, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(hours = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbygroup_hour(dic["groupname"], cDate, hourNum + 1, cnt)


def statsbydn(cDate):
    """
    按日志分类统计数据量
    """

    es = estats.ESData(configFile)
    if es.Exists(cDate):     # 索引是否存在
        #lst = postdata.get_dn_list()
        lst = postdata.get_dn_list_day(cDate)
        # 按日志分类统计数据量
        statsbydn_day(es, lst, cDate)

        # 按日志分类统计分小时的数据量
        for i in range(24):
            statsbydn_hour(es, lst, cDate, i)

def statsbydn_day(es, lst, cDate):
    for dic in lst:
        cond = FIELD_DN + ":\"" + dic["dn"] + "\""

        sTime = datetime.time(hour=0, minute=0, second=0)
        #eTime = datetime.time(hour=23, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(days = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbydn_day(dic["dn"], cDate, cnt)

def statsbydn_hour(es, lst, cDate, hourNum):
    for dic in lst:
        cond = FIELD_DN + ":\"" + dic["dn"] + "\""

        sTime = datetime.time(hour=hourNum, minute=0, second=0)
        #eTime = datetime.time(hour=hourNum, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(hours = 1)

        cnt = es.Count(sDatetime, eDatetime, cond)
        postdata.new_statsbydn_hour(dic["dn"], cDate, hourNum + 1, cnt)


def statsbynode(cDate):
    """
    按流量采集结点统计数据量
    """

    es = estats.ESData(configFile)
    if es.Exists(cDate, idxType = "flows"):     # 索引是否存在
        # 获取某日流量采集结点列表
        lst = es.Get_node_list_day(cDate)
        # 按流量采集结点统计数据量
        statsbynode_day(es, lst, cDate)

        # 按流量采集结点统计分小时的数据量
        for i in range(24):
            statsbynode_hour(es, lst, cDate, i)

def statsbynode_day(es, lst, cDate):
    for dic in lst:
        cond = FIELD_NODE + ":\"" + dic["key"] + "\""

        sTime = datetime.time(hour=0, minute=0, second=0)
        #eTime = datetime.time(hour=23, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(days = 1)

        flowscnt = es.Count(sDatetime, eDatetime, cond, idxType = "flows")
        flowsbytes = es.Sum(sDatetime, eDatetime, FIELD_BYTES, searchCond = cond, idxType = "flows")
        flowspackets = es.Sum(sDatetime, eDatetime, FIELD_PACKETS, searchCond = cond, idxType = "flows")
        postdata.new_statsbynode_day(dic["key"], cDate, flowscnt, flowsbytes, flowspackets)

def statsbynode_hour(es, lst, cDate, hourNum):
    for dic in lst:
        cond = FIELD_NODE + ":\"" + dic["key"] + "\""

        sTime = datetime.time(hour=hourNum, minute=0, second=0)
        #eTime = datetime.time(hour=hourNum, minute=59, second=59)
        sDatetime = datetime.datetime.combine(cDate, sTime)
        #eDatetime = datetime.datetime.combine(cDate, eTime)
        eDatetime = sDatetime + datetime.timedelta(hours = 1)

        flowscnt = es.Count(sDatetime, eDatetime, cond, idxType = "flows")
        flowsbytes = es.Sum(sDatetime, eDatetime, FIELD_BYTES, searchCond = cond, idxType = "flows")
        flowspackets = es.Sum(sDatetime, eDatetime, FIELD_PACKETS, searchCond = cond, idxType = "flows")

        postdata.new_statsbynode_hour(dic["key"], cDate, hourNum + 1, flowscnt, flowsbytes, flowspackets)


def funcNoon(pd):
    """
    每日中午12:00要执行的任务
    """

    if pd != None and pd.is_master() == False:
        return

    try:
        execTime = datetime.datetime.now()
        cDateTime = execTime + datetime.timedelta(days = -1)
        cDate = cDateTime.date()

        stats(cDate)
        statsbyhost(cDate)
        statsbygroup(cDate)
        statsbydn(cDate)
        statsbynode(cDate)
    except Exception, e:
        sys_log.SysLog(postdata._logFile, postdata._instance).writeLog("error", str(traceback.format_exc()))

def funcHour(pd):
    """
    每小时后的第5分钟要执行的任务
    """

    try:
        if pd != None and pd.is_master() == False:
            return

        execTime = datetime.datetime.now()
        cDateTime = execTime + datetime.timedelta(hours = -1)
        cDate = cDateTime.date()
        hourNum = cDateTime.hour

        lstHost = postdata.get_host_list_day(cDate)
        lstGroup = postdata.get_group_list_day(cDate)
        lstDn = postdata.get_dn_list_day(cDate)

        es = estats.ESData(configFile)
        # 获取某日流量采集结点列表
        lstNode = es.Get_node_list_day(cDate)

        stats_hour(es, cDate, hourNum)

        if es.Exists(cDate, idxType = "logs"):   # 判断日志索引是否存在
            statsbyhost_hour(es, lstHost, cDate, hourNum)
            statsbygroup_hour(es, lstGroup, cDate, hourNum)
            statsbydn_hour(es, lstDn, cDate, hourNum)

        if es.Exists(cDate, idxType = "flows"):   # 判断流量索引是否存在
            # 按流量采集结点统计数据量
            statsbynode_hour(es, lstNode, cDate, hourNum)

        # 一小时一小时往前推，判断前面的数据是否统计过
        # 直到索引不存在或统计数据已经存在
        dDateTime = cDateTime + datetime.timedelta(hours = -1)
        dDate = dDateTime.date()
        hNum = dDateTime.hour
        while (es.Exists(dDate, idxType = "logs") or es.Exists(dDate, idxType = "flows")) \
                and postdata.is_stats_data(dDate, hNum + 1) == False:
            if dDate != cDate:
                lstHost = postdata.get_host_list_day(dDate)
                lstGroup = postdata.get_group_list_day(dDate)
                lstDn = postdata.get_dn_list_day(dDate)
                lstNode = es.Get_node_list_day(dDate)
                cDate = dDate

            stats_hour(es, dDate, hNum)
            statsbyhost_hour(es, lstHost, dDate, hNum)
            statsbygroup_hour(es, lstGroup, dDate, hNum)
            statsbydn_hour(es, lstDn, dDate, hNum)
            statsbynode_hour(es, lstNode, dDate, hNum)

            dDateTime = dDateTime + datetime.timedelta(hours = -1)
            dDate = dDateTime.date()
            hNum = dDateTime.hour
    except Exception, e:
        sys_log.SysLog(postdata._logFile, postdata._instance).writeLog("error", str(traceback.format_exc()))

def funcMidnight(pd):
    """
    每日午夜后的第10分钟要执行的任务
    """

    try:
        if pd != None and pd.is_master() == False:
            return

        execTime = datetime.datetime.now()
        cDateTime = execTime + datetime.timedelta(days = -1)
        cDate = cDateTime.date()

        es = estats.ESData(configFile)
        stats_day(es, cDate)

        if es.Exists(cDate):
            # 优化索引
            es.Optimize(cDate)
            # 统计昨日的数据
            lstHost = postdata.get_host_list_day(cDate)
            lstGroup = postdata.get_group_list_day(cDate)
            lstDn = postdata.get_dn_list_day(cDate)

            statsbyhost_day(es, lstHost, cDate)
            statsbygroup_day(es, lstGroup, cDate)
            statsbydn_day(es, lstDn, cDate)

        if es.Exists(cDate, idxType = "flows"):
            # 优化索引
            es.Optimize(cDate, idxType = "flows")
            # 统计昨日的数据
            lstNode = es.Get_node_list_day(cDate)

            statsbynode_day(es, lstNode, cDate)

        # 一天一天往前推，判断前面的数据是否统计过
        # 直到索引不存在或统计数据已经存在
        dDate = cDate + datetime.timedelta(days = -1)
        while (es.Exists(dDate, idxType = "logs") or es.Exists(dDate, idxType = "flows")) and \
                postdata.is_stats_data(dDate) == False:
            lstHost = postdata.get_host_list_day(dDate)
            lstGroup = postdata.get_group_list_day(dDate)
            lstDn = postdata.get_dn_list_day(dDate)
            lstNode = es.Get_node_list_day(dDate)

            stats_day(es, dDate)
            statsbyhost_day(es, lstHost, dDate)
            statsbygroup_day(es, lstGroup, dDate)
            statsbydn_day(es, lstDn, dDate)
            statsbynode_day(es, lstNode, dDate)

            dDate = dDate + datetime.timedelta(days = -1)

        # 删除已过期的索引
        ret = postdata.get_retain(idxType = "logs")
        if ret > 0:
            es.Delete_Overdue_Indexes(ret, cDate, idxType = "logs")

        ret = postdata.get_retain(idxType = "flows")
        if ret > 0:
            es.Delete_Overdue_Indexes(ret, cDate, idxType = "flows")
    except Exception, e:
        sys_log.SysLog(postdata._logFile, postdata._instance).writeLog("error", str(traceback.format_exc()))

def run(start_type = None):
    global postdata

    conf = sys_config.SysConfig(configFile)
    # 进程号文件名
    pidFile = conf.getConfig("datastats", "pidFile")

    if start_type == None:
        if os.path.exists(pidFile):
            os.remove(pidFile)
        pFile = open(pidFile, "w")
        pFile.write(str(os.getpid()))
        pFile.close()

    # 生成postdata对象
    postdata = postgrestats.PostgresData(configFile)
    argus = [ postdata ]

    sched = Scheduler(standalone = True)

    sched.add_cron_job(funcNoon, year=cron_noon[0], month=cron_noon[1], \
            week=cron_noon[2], day_of_week=cron_noon[3], day=cron_noon[4], \
            hour=cron_noon[5], minute=cron_noon[6], second=cron_noon[7], args=argus)
    sched.add_cron_job(funcHour, year=cron_hour[0], month=cron_hour[1], \
            week=cron_hour[2], day_of_week=cron_hour[3], day=cron_hour[4], \
            hour=cron_hour[5], minute=cron_hour[6], second=cron_hour[7], args=argus)
    sched.add_cron_job(funcMidnight, year=cron_midnight[0], month=cron_midnight[1], \
            week=cron_midnight[2], day_of_week=cron_midnight[3], day=cron_midnight[4], \
            hour=cron_midnight[5], minute=cron_midnight[6], second=cron_midnight[7], args=argus)
    # 自定义dashboard统计服务
    sched.add_cron_job(stats_dashboard.stats_dashboard, year=cron_dashboard[0], month=cron_dashboard[1], \
            week=cron_dashboard[2], day_of_week=cron_dashboard[3], day=cron_dashboard[4], \
            hour=cron_dashboard[5], minute=cron_dashboard[6], second=cron_dashboard[7], args=argus)

    # 每隔几分钟（默认5分钟）检查是否需要删除原始pcap文件
    interval_chkdevice = conf.getConfig("datastats", "intervalCheckDevice")
    if interval_chkdevice == None:
        interval_chkdevice = 5
    else:
        interval_chkdevice = int(interval_chkdevice)

    sched.add_interval_job(chkdevice.checkDevice, weeks=0, days=0, hours=0, minutes=interval_chkdevice, seconds=0, args=argus)

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass

# 测试用
if __name__ == "__main__":
    
    run()

