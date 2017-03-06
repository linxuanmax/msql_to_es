#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-08-26"

import os
import time
import traceback
import datetime

from apscheduler.scheduler import Scheduler
from apscheduler import events, job

import scheduletime
import postgrestasksched
import esdata
from utils import sys_config, sys_log


# TODO
# 该提示在实际部署时要删除
# 默认字段是否需要修改？是否需要加入到配置文件中？

# 默认字段
DEFAULTFIELDS = [ "msg.message" ]
#DEFAULTFIELDS = [ "@message" ]
# 配置文件
#configFile = sys_config.getDir() + "/conf/mdstack.conf"
configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
if os.path.exists(configFile) == False:
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
DELIMITER = "$*$*$"
# 计划任务字典
dicTask = {}
# job字典
dicJob = {}
# postgres数据处理对象
postdata = None
# 循环检测数据源配置时间间隔
LOOPTIME = 60


def taskFunc( schedid, searchcond, startime, endtime, warnornot, warncondop, warncondval, warnlevel, saveresult ):
    """
    计划任务的实际功能
    """
    
    if postdata != None and postdata.is_master() == False:
        return

    execTime = datetime.datetime.now()
    #print 'Task start time: %s' % execTime.strftime( '%Y-%m-%d %H:%M:%S' )

    # 要将searchstart和searchend转换为绝对时间；原类型为字符串
    searchstart = scheduletime.fmtRelTime( startime, execTime )
    #print 'Condition start time: %s' % searchstart.strftime( '%Y-%m-%d %H:%M:%S' )
    searchend = scheduletime.fmtRelTime( endtime, execTime )
    #print 'Conditon end time: %s' % searchend.strftime( '%Y-%m-%d %H:%M:%S' )
    
    try:
        es = esdata.ESData( configFile )
        # 查询符合条件的数据量
        cnt = es.Count( searchcond, searchstart, searchend )
        #print 'Count of search: %s' % cnt
        
        if saveresult > 0:
            #lst = es.Search( searchcond, searchstart, searchend, fields = DEFAULTFIELDS, size = saveresult )
            lst = es.Search( searchcond, searchstart, searchend, fields = None, size = saveresult )
            #print 'Length of search list: %s' % len( lst )

        # 向t_schedresult中插入一条数据，并返回插入数据的id（resultid）
        resultid = postdata.newSchedResult( schedid, execTime, searchcond, searchstart, searchend, cnt, saveresult )

        warnid = 0
        if warnornot.upper() != 'N':
            if ( warncondop == '&gt;' and cnt > warncondval ) or \
                    ( warncondop == '&gt;=' and cnt >= warncondval ) or \
                    ( warncondop == '&lt;=' and cnt <= warncondval ) or \
                    ( warncondop == '&lt;' and cnt < warncondval ) or \
                    ( warncondop == '=' and cnt == warncondval ):
                        # 向t_warn中插入报警数据，并返回插入数据的id（warnid)
                        warnid = postdata.newWarn( schedid, execTime, warnlevel, searchcond, searchstart, searchend, cnt )
        
        # 向t_resultdetail中插入数据
        if saveresult > 0 and len( lst ) > 0:
            postdata.newResultDetail( warnid, resultid, lst )
    except Exception, e:
        conf = sys_config.SysConfig( configFile )
        _logFile = conf.getConfig( "tasksched", "logFile" )
        _instance = conf.getConfig( "tasksched", "instanceName" )
        sys_log.SysLog( _logFile, _instance ).writeLog( "error", str( traceback.format_exc() ) )


def taskListener( event ):
    """
    计划任务的监听器,用于停止已经到结束时间的任务
    """

    if dicJob[event.job.name] != None:
        # 如果存在计划终止时间
        if event.job.next_run_time > dicJob[event.job.name]:
            # 如果计划的下次执行时间大于计划终止时间
            taskid = event.job.name.replace( 'Job', '' )
            # 停止计划任务
            dicTask['Task'+taskid].shutdown( wait=False )
            # 设置计划任务的状态标志为过期
            postdata.expireschedule( int(taskid) )
            # 从字典中移除
            del dicJob[event.job.name]
            del dicTask['Task'+taskid]


def createTask( listTask ):
    """
    创建并开始计划任务
    """

    for dic in listTask:
        taskID = str( dic["schedid"] )
        searchCond = dic["searchcond"].replace(DELIMITER, " ")
        searchStart = dic["searchstart"]
        searchEnd = dic["searchend"]
        schedStart = dic["schedstart"]
        schedEnd = dic["schedend"]
        schedTime = dic["schedtime"]
        schedCron = dic["schedcron"]
        warnOrNot = dic["warnornot"]
        warnCondOp = dic["warncondop"]
        warnCondVal = dic["warncondval"]
        warnLevel = dic["warnlevel"]
        saveResult = dic["saveresult"]

        argus = [ int(taskID), searchCond, searchStart, searchEnd, warnOrNot, warnCondOp, warnCondVal, warnLevel, saveResult ]

        sched = Scheduler()
        if schedTime != None:
            ( sWeeks, sDays, sHours, sMinutes, sSeconds ) = scheduletime.fmtSchedTime( schedTime )
            if schedStart == None:    # interval_job 在start_date为None时默认从当前算起，过一个设定的时间间隔第一次执行任务
                schedStart = datetime.datetime.now() + datetime.timedelta( seconds = 2 ) - datetime.timedelta( seconds = sSeconds, \
                        minutes = sMinutes, hours = sHours, days = sDays, weeks = sWeeks )
            sched.add_interval_job( taskFunc, weeks=sWeeks, days=sDays, hours=sHours, minutes=sMinutes, seconds=sSeconds, \
                    start_date=schedStart, name='Job'+taskID, args=argus )
        elif schedCron != None:
            ( cSecond, cMinute, cHour, cDay, cDayofWeek, cWeek, cMonth, cYear ) = scheduletime.frmSchedCron( schedCron )
            sched.add_cron_job( taskFunc, year=cYear, month=cMonth, week=cWeek, day_of_week=cDayofWeek, day=cDay, \
                    hour=cHour, minute=cMinute, second=cSecond, start_date=schedStart, name='Job'+taskID, args=argus )
        sched.add_listener( taskListener, events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR )

        # 保存计划任务的截止时间
        dicJob['Job'+taskID] = schedEnd

        dicTask['Task'+taskID] = sched
        dicTask['Task'+taskID].start()
        #print 'Task %s start time is: %s' % ( taskID, datetime.datetime.now() )


def deleteTask(listTask):
    """
    停止并删除计划任务
    """

    for dic in listTask:
        taskID = str( dic["schedid"] )
        if dicTask.has_key( 'Task'+taskID ):
            # 停止计划任务
            dicTask['Task'+taskID].shutdown( wait=False )
            # 从字典中移除
            del dicJob["Job"+taskID]
            del dicTask['Task'+taskID]


def run(start_type = None):
    global dicTask, dicJob, postdata

    conf = sys_config.SysConfig(configFile)
    # 进程号文件名
    pidFile = conf.getConfig("tasksched", "pidFile")

    if start_type == None:
        if os.path.exists(pidFile):
            os.remove(pidFile)
        pFile = open(pidFile, "w")
        pFile.write(str(os.getpid()))
        pFile.close()

    # 生成postdata对象
    postdata = postgrestasksched.PostgresData( configFile )

    # 清空表t_schednew、t_schedupd、t_scheddel
    postdata.clearSchedule()

    # 读取活动的计划任务
    lstTask = postdata.getTask()
    
    if lstTask != None and len( lstTask ) > 0:
        # 创建并启动计划任务
        #print "Length of lstTask is: %s" % len( lstTask )
        createTask( lstTask )

    while True:
        # 延时60秒
        time.sleep( LOOPTIME )

        # 读取变化过的计划任务
        lstNew = postdata.getNewTask()
        lstUpd = postdata.getUpdTask()
        lstDel = postdata.getDelTask()

        # 清空表t_schednew、t_schedupd、t_scheddel 
        postdata.clearSchedule()

        # 创建新的计划任务
        if lstNew != None and len( lstNew ) > 0:
            createTask( lstNew )

        # 修改已存在的计划任务
        if lstUpd != None and len( lstUpd ) > 0:
            # 先停止
            deleteTask( lstUpd )
            # 再重建
            createTask( lstUpd )

        # 删除计划任务
        if lstDel != None and len( lstDel ) > 0:
            deleteTask( lstDel )


#测试用
if __name__ == "__main__":

    run()
