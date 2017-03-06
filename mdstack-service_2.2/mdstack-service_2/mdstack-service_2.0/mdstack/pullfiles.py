#!usr/bin/env python
# -*- coding:utf-8 -*-

__author__="fallmoon"
__date__="$2013-09-27"

import os
import time
import hashlib
import traceback
import datetime
import base64

from apscheduler.scheduler import Scheduler
from apscheduler import events, job

import scheduletime
import postgrespullfile
from filetools import fileftp, filessh, filesmb
from utils import sys_config, sys_log


# TODO
# 该提示在实际部署时要删除
# 用户密码是经过base64编码过的，需要解码


# 配置文件
#configFile = sys_config.getDir() + "/conf/mdstack.conf"
configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
if os.path.exists(configFile) == False:
    configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
# 计划任务字典
dicTask = {}
# job字典
dicJob = {}
# postgres数据处理对象
postdata = None
# 循环检测数据源配置时间间隔
LOOPTIME = 60


def getFileHash( filepath, hashname='sha1', buf=4096 ):
    """
    计算文件的HASH值
    hashname是哈希值算法的名称: md5/sha1/sha224/sha256/sha384/sha512
    """
    
    bs = None
    hashcode = hashlib.new( hashname )
    with open( filepath, 'rb' ) as f:
        while True:
            bs = f.read( buf )
            if not bs:
                break
            else:
                hashcode.update( bs )

    hashvalue = hashcode.hexdigest()
    #print 'The hash value of file %s is: %s' % ( filepath, hashvalue )
    return hashvalue


def taskFunc( pfid, groupid, configpath, logsource, protocol, port, username, userpass, fpath, files ):
    """
    文件提取任务的实际功能
    """

    execTime = datetime.datetime.now()
    #print 'Task start time: %s' % execTime.strftime( '%Y-%m-%d %H:%M:%S' )
    #print 'Task function parameters: %s' %( [pfid, groupid, configpath, logsource, protocol, port, username, userpass, fpath, files] )
    
    # 读取配置
    conf = sys_config.SysConfig( configFile )
    rootPath = conf.getConfig( "pullfile", "rootPath" )
    tmpPath = os.path.join( rootPath, 'tmp' )

    # 读取组名
    groupName = postdata.getGroupName( groupid )

    if username == None:
        username = ''
    if userpass == None:
        userpass = ''
    else:
        #userpass = userpass 
        userpass = base64.decodestring( userpass )

    obj = None
    if protocol.lower() == 'ftp':
        # FTP
        if port == None:
            port = 21
        obj = fileftp.FileFtp( username, userpass, logsource, port )
    elif protocol.lower() == 'samba':
        # SMB
        if port == None:
            port = 139
        obj = filesmb.FileSMB( username, userpass, logsource, port )
    elif protocol.lower() == 'ssh':
        # SSH
        if port == None:
            port = 22
        obj = filessh.FileSSH( {'username': username, 'password': userpass}, logsource, port )

    if obj != None:
        lstFile = obj.GetFileList( os.path.join( fpath, files ) )
        for remoteFile in lstFile:
            lstName = os.path.split( remoteFile )
            localFile = os.path.join( tmpPath, lstName[1] )
            obj.DownLoadFile( localFile, remoteFile )
            if os.path.exists( localFile ):
                hashvalue = getFileHash( localFile )
                if postdata.existsFileHash( hashvalue ):
                    os.remove( localFile )
                else:
                    #lastmoditime = datetime.datetime.now()
                    lastmoditime = obj.getFileModiTime( remoteFile )
                    postdata.insFileHash( hashvalue, lstName[1], lastmoditime, configpath, logsource )
                    fileName = hashvalue
                    lstExt = lstName[1].split('.')
                    if len(lstExt) > 1:
                        fileName = hashvalue + '.' + lstExt[len(lstExt) - 1]
                    
                    dstPath = os.path.join( rootPath, 'pull', groupName, execTime.strftime('%Y%m%d'), logsource.replace(':','-'), configpath )
                    if os.path.isdir( dstPath ) == False:
                        os.makedirs( dstPath )

                    os.rename( localFile, os.path.join( dstPath, fileName ) )
 
        obj.close()


def taskListener( event ):
    """
    计划任务的监听器,用于停止已经到结束时间的任务
    """

    #print 'Task event job name: %s' %( event.job.name )
    pfid = event.job.name.replace( 'Job', '' )
    if dicJob['T'+pfid] == 'Y':  # 只执行一次
        # 停止计划任务
        dicTask['Task'+pfid].shutdown( wait=False )
        # 设置任务的状态标志为完成
        postdata.setschedulestatus( int(pfid), 1 )      # 1 完成 2 失效 到期
        # 从字典中移除
        del dicJob['T'+pfid]
        del dicJob[event.job.name]
        del dicTask['Task'+pfid]
    elif dicJob[event.job.name] != None:
        # 如果存在计划终止时间
        if event.job.next_run_time > dicJob[event.job.name]:
            # 停止计划任务
            dicTask['Task'+pfid].shutdown( wait=False )
            # 设置任务的状态标志为过期
            postdata.setschedulestatus( int(pfid), 2 )  # 1 完成 2 失效 到期
            # 从字典中移除
            del dicJob['T'+pfid]
            del dicJob[event.job.name]
            del dicTask['Task'+pfid]


def createTask( listTask ):
    """
    创建并开始文件提取任务
    """

    for dic in listTask:
        pfID = str(dic["pfid"])
        groupID = str(dic["groupid"])
        configPath = dic["configpath"]
        logSource = dic["logsource"]
        Protocol = dic["protocol"]
        Port = dic["port"]
        userName = dic["username"]
        userPass = dic["userpass"]
        fPath = dic["fpath"]
        Files = dic["files"]
        oneTime = dic["onetime"]
        schedStart = dic["schedstart"]
        schedEnd = dic["schedend"]
        schedTime = dic["schedtime"]
        schedCron = dic["schedcron"]

        argus = [ int(pfID), int(groupID), configPath, logSource, Protocol, Port, userName, userPass, fPath, Files ]
        
        sched = Scheduler()
        if oneTime.upper() == 'Y':  # 只执行一次
            if schedStart == None:
                schedStart = datetime.datetime.now() + datetime.timedelta( seconds = 2 )  # 延时两秒
            sched.add_date_job( taskFunc, schedStart, name='Job'+pfID, args=argus )
        elif schedTime != None:
            ( sWeeks, sDays, sHours, sMinutes, sSeconds ) = scheduletime.fmtSchedTime( schedTime )
            if schedStart == None:    # interval_job 在start_date为None时默认从当前算起，过一个设定的时间间隔第一次执行任务
                schedStart = datetime.datetime.now() + datetime.timedelta( seconds = 2 ) - datetime.timedelta( seconds = sSeconds, \
                        minutes = sMinutes, hours = sHours, days = sDays, weeks = sWeeks )
            sched.add_interval_job( taskFunc, weeks=sWeeks, days=sDays, hours=sHours, minutes=sMinutes, seconds=sSeconds, \
                    start_date=schedStart, name='Job'+pfID, args=argus )
        elif schedCron != None:
            ( cSecond, cMinute, cHour, cDay, cDayofWeek, cWeek, cMonth, cYear ) = scheduletime.frmSchedCron( schedCron )
            sched.add_cron_job( taskFunc, year=cYear, month=cMonth, week=cWeek, day_of_week=cDayofWeek, day=cDay, \
                    hour=cHour, minute=cMinute, second=cSecond, start_date=schedStart, name='Job'+pfID, args=argus )
        sched.add_listener( taskListener, events.EVENT_JOB_EXECUTED | events.EVENT_JOB_ERROR )

        # 保存计划任务的截止时间
        dicJob['T'+pfID] = oneTime.upper()
        dicJob['Job'+pfID] = schedEnd

        dicTask['Task'+pfID] = sched
        dicTask['Task'+pfID].start()
        #print 'Task %s start time is: %s' % (pfID, datetime.datetime.now())


def deleteTask( listTask ):
    """
    停止并删除文件读取任务
    """

    for dic in listTask:
        pfID = str( dic["pfid"] )
        if dicTask.has_key( 'Task'+pfID ):
            # 停止计划任务
            dicTask['Task'+pfID].shutdown( wait=False )
            # 从字典中移除
            del dicJob["T"+pfID]
            del dicJob["Job"+pfID]
            del dicTask['Task'+pfID]


def run(start_type = None):
    global dicTask, dicJob, postdata
    
    conf = sys_config.SysConfig(configFile)
    # 进程号文件名
    pidFile = conf.getConfig("pullfile", "pidFile")

    if start_type == None:
        if os.path.exists(pidFile):
            os.remove(pidFile)
        pFile = open(pidFile, "w")
        pFile.write(str(os.getpid()))
        pFile.close()

    # 生成postdata对象
    postdata = postgrespullfile.PostgresData( configFile )

    # 清空表t_pfnew、t_pfupd、t_pfdel
    postdata.clearSchedule()

    # 读取活动的文件提取任务
    lstTask = postdata.getTask()

    if lstTask != None and len( lstTask ) > 0:
        # 创建并启动文件提取任务
        #print "Length of lstTask is: %s" % len( lstTask )
        createTask( lstTask )

    while True:
        # 延时60秒
        time.sleep( LOOPTIME )

        # 读取变化过的文件提取任务
        lstNew = postdata.getNewTask()
        lstUpd = postdata.getUpdTask()
        lstDel = postdata.getDelTask()

        # 清空表t_pfnew、t_pfupd、t_pfdel
        postdata. clearSchedule()

        # 创建新的文件提取任务
        if lstNew != None and len( lstNew ) > 0:
            createTask( lstNew )

        # 修改已存在的文件提取任务
        if lstUpd != None and len( lstUpd ) > 0:
            # 先停止
            deleteTask( lstUpd )
            # 再重建
            createTask( lstUpd )

        # 删除文件提取任务
        if lstDel != None and len( lstDel ) > 0:
            deleteTask( lstDel )

        # 如果没有文件提取任务，则提出
        #if len( dicTask ) == 0:
        #    return


#测试用
if __name__ == "__main__":
    
    run()
