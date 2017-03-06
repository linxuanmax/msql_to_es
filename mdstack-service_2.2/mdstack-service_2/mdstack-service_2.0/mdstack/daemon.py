#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__ ="$2013-10-30$"

import sys
import os
import time
import traceback
import re
#from signal import SIGTERM
import signal

from utils import sys_log
import dataextraction
import pullfiles
import taskscheduler
import datastats


class Daemon:
    """
    A generic daemon class.
    Usage: subclass the Daemon class and override the run() method
    """

    def __init__(self, service, pidfile, logfile, instance, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.service = service
        self.logfile = logfile
        self.instance = instance

        if self.service == 'taskscheduler':
            self.serviceName = '[*] The Task Scheduler Service'
        elif self.service == 'datastats':
            self.serviceName = '[*] The Data Statistics Service'
        elif self.service == 'dataextraction':
            self.serviceName = '[*] The  Data Extraction Service'
        elif self.service == 'pullfiles':
            self.serviceName = '[*] The Pull Files Service'


    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced
        Programming in the UNIX Environment" for details (ISBN 0201563177)
        """

        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # write pidfile
        #atexit.register(self.delpid)
        #pid = str(os.getpid())
        #file(self.pidfile,'w+').write("%s" % pid)


    def delpid(self):
        os.remove(self.pidfile)


    def start(self):
        """
        Start the daemon
        """

        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile,'r')
            pid = pf.read().strip()
            pf.close()
        except IOError:
            pid = None

        try:
            if pid:
                message = self.serviceName + " already running!"
                print message
                sys_log.SysLog(self.logfile, self.instance).writeLog("error", message)
                sys.exit(1)

            print self.serviceName + " is starting ..."

            # Start the daemon
            self.daemonize()
            self.run()
        except Exception,e:
            sys_log.SysLog(self.logfile, self.instance).writeLog("error",str(traceback.format_exc()))


    def stop(self):
        """
        Stop the daemon
        """

        # Get the pid from the pidfile
        try:
            pf = file(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = self.serviceName + ' may not be running. The pid file "%s" does not exist.'
            print message % self.pidfile
            return # not an error in a restart

        try:
            #os.kill(pid, signal.SIGTERM)
            print "[*] please waitting ..."

            if self.service == 'taskscheduler':
                time.sleep(1)
                
                while True:
                    ps = os.popen("ps -ef|grep 'root.*" + str(pid) + ".*python.*taskscheduler.py --[auto|re]*start'","r").read()
                    ps = ps.splitlines()
                    if len(ps) > 0:
                        for p in ps:
                            pid = int((re.findall("root\s*(\d*)\s*", p))[0])
                            os.kill(pid, signal.SIGKILL)
                    else:
                        break
            elif self.service == 'datastats':
                time.sleep(1)

                while True:
                    ps = os.popen("ps -ef|grep 'root.*" + str(pid) + ".*python.*datastats.py --[auto|re]*start'","r").read()
                    ps = ps.splitlines()
                    if len(ps) > 0:
                        for p in ps:
                            pid = int((re.findall("root\s*(\d*)\s*", p))[0])
                            os.kill(pid, signal.SIGKILL)
                    else:
                        break
            elif self.service == 'dataextraction':
                time.sleep(1)
                
                while True:
                    ps = os.popen("ps -ef|grep 'root.*" + str(pid) + ".*python.*dataextraction.py --[auto|re]*start'","r").read()
                    ps = ps.splitlines()
                    if len(ps) > 0:
                        for p in ps:
                            pid = int((re.findall("root\s*(\d*)\s*", p))[0])
                            os.kill(pid, signal.SIGKILL)
                    else:
                        break
            elif self.service == 'pullfiles':
                time.sleep(1)

                while True:
                    ps = os.popen("ps -ef|grep 'root.*" + str(pid) + ".*python.*pullfiles.py --[auto|re]*start'","r").read()
                    ps = ps.splitlines()
                    if len(ps) > 0:
                        for p in ps:
                            pid = int((re.findall("root\s*(\d*)\s*", p))[0])
                            os.kill(pid, signal.SIGKILL)
                    else:
                        break
        except OSError, err:
            print err
            pass 

        if os.path.exists(self.pidfile):
            os.remove(self.pidfile)

        print self.serviceName + " is stoped!"


    def restart(self):
        """
        Restart the daemon
        """
        try:
            pf = file(self.pidfile,'r')
            pid = pf.read().strip()
            pf.close()
        except IOError:
            pid = None

        if pid:
            self.stop()
            time.sleep(1)
            self.start()
        else:
            self.start()
            """
            message = "Could not restart! " + self.serviceName + " may not be running."
            print message + "\n"
            sys_log.SysLog(self.logfile, self.instance).writeLog("error", message)
            """


    def status(self):
        """
        Check the status of the process (RUNNING | STOPPED)
        """

        try:
            pf = file(self.pidfile,'r')
            pid = pf.read().strip()
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = self.serviceName + ": RUNNING"
        else:
            message = self.serviceName + ": STOPPED"

        print message
        sys.exit(1)


    def run(self):
        #sig = sighandler.SignalHandler()
        #sig.register(signal.SIGTERM, sig_exit)
        
        if self.service == 'taskscheduler':
            taskscheduler.run()
        elif self.service == 'datastats':
            datastats.run()
        elif self.service == 'dataextraction':
            dataextraction.run()
        elif self.service == 'pullfiles':
            pullfiles.run()

