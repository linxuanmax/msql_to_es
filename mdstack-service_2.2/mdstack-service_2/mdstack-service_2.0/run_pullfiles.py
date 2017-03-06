#!/opt/secsphere/bin/python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__ ="$2013-10-31$"


from mdstack import daemon
from mdstack.utils import sys_config, sys_log
from getopt import getopt
from getopt import GetoptError
import sys
import os

def main():
    # 配置文件
    #configFile = sys_config.getDir() + "/mdstack/conf/mdstack.conf"
    configFile = os.path.split(os.path.realpath(__file__))[0] + "/mdstack/conf/mdstack.conf"
    if os.path.exists(configFile) == False:
        configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"
    serviceName = "pullfiles"
    confZone = "pullfile"

    conf = sys_config.SysConfig(configFile)

    # 进程号文件名
    pidFile = conf.getConfig(confZone, "pidFile")
    # 日志文件
    logFile = conf.getConfig(confZone, "logFile")
    # 实例名
    instance = conf.getConfig(confZone, "instanceName")

    daemon1 = daemon.Daemon(serviceName, pidFile, logFile, instance)

    try:
        lstPar, lstErrPar = getopt(sys.argv[1:], "", ["start","autostart","stop","restart","status","help"])
        if len(lstErrPar) > 0:
            print "Unknown command, try \"python run_pullfiles.py --help\" for more information."
        elif len(lstPar) <> 1:
            print "Unknown command, try \"python run_pullfiles.py --help\" for more information."
        else:
            if lstPar[0][0] == "--start":
                sys_log.SysLog(logFile, instance).writeLog("info", "The Pull Files Service is starting ...")
                daemon1.start()
            elif lstPar[0][0] == "--autostart":
                sys_log.SysLog(logFile, instance).writeLog("info", "The Pull Files Service is starting ...")
                daemon1.start()
            elif lstPar[0][0] == "--stop":
                daemon1.stop()
                sys_log.SysLog(logFile, instance).writeLog("info", "The Pull Files Service is stopped")
            elif lstPar[0][0] == "--restart":
                sys_log.SysLog(logFile, instance).writeLog("info", "The Pull Files Service is restarting ...")
                daemon1.restart()
            elif lstPar[0][0] == "--status":
                daemon1.status()
            elif lstPar[0][0] == "--help":
                print "Usage: python run_pullfiles.py {Options}"
                print "Options:"
                print "    --start:     Start the Pull Files Service"
                print "    --stop:      Stop the Pull Files Service"
                print "    --restart:   Restart the Pull Files Service"
                print "    --status:    Show the status of the Pull Files Service"
                print "    --help:      Show the help"
    except GetoptError, e:
        print e.msg + ", try \"run_pullfiles.py --help\" for more information."

if __name__ == "__main__":
    main()

