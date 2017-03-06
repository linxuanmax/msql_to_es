#!/usr/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="2014-07-07"

import os
import sys
import socket
import traceback
import pyes

#sys.path.append("..")
from mdstack.utils import sys_config, sys_log

class NoConfigFile(Exception):
    pass

class chkDevice():
    """
    检测pcap文件存储位置的剩余空间是否小于设置的最小剩余空间
    当剩余空间过小时，删除最早存储的pcap文件，直到剩余空间足够
    """

    def __init__(self, cfgFile = None):
        self._configFile = cfgFile
        if self._configFile == None:
            #self._configFile = sys_config.getDir() + "/conf/mdstack.conf"
            self._configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/mdstack.conf"
            #print self._configFile
            if os.path.exists(self._configFile) == False:
                self._configFile = "/opt/mdstack/conf/mdstackd/mdstack.conf"

        if os.path.exists(self._configFile) == False:
            raise NoConfigFile  # 暂不捕获该错误，直接退出

        conf = sys_config.SysConfig(self._configFile)

        eshost = conf.getConfig("esflow", "esHost")
        esport = conf.getConfig("esflow", "esPort")
        self._url = 'http://' + eshost + ":" + esport

        # 日志文件
        self._logFile = conf.getConfig("datastats", "logFile")

        # 实例名
        self._instance = conf.getConfig("datastats", "instanceName")

        # 抓包配置文件
        # 在配置文件mdstack.conf的datastats段，增加一个抓包配置文件位置的配置项
        self._capConf = conf.getConfig("datastats", "captureConfigFile")
        if self._capConf == None:
            self._capConf = "/usr/local/etc/capture/config.ini"

        self._confCap = sys_config.SysConfig(self._capConf)


    def hostnameToNodeids(self, hostname):
        """
        通过本机的主机名获取结点id列表
        """

        nodes = []

        try:
            # MatchQuery
            # MatchQuery 替代了 TextQuery
            query = pyes.query.MatchQuery("hostname", hostname)

            # 连接ES
            es = pyes.ES( self._url )

            search = pyes.query.Search( query=query, start=0, size=100, fields=[] )
            results = es.search( search, indices="stats", doc_types="stat" )
            if results != None:
                for r in results.hits:
                    nodes.append( r._id )
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )

        return nodes


    def getFull(self, node, key, defaultValue):
        """
        从抓包配置文件中读取相应配置
        """
        
        ret = self._confCap.getConfig(node, key)
        if ret != None:
            return ret

        nClass = self._confCap.getConfig(node, "nodeClass")
        if nClass != None:
            ret = self._confCap.getConfig(nClass, key)
            if ret != None:
                return ret

        ret = self._confCap.getConfig("default", key)
        if ret != None:
            return ret

        return defaultValue


    def getFreeDiskSpace(self, pcapDir):
        """
        获取文件所在路径的空余空间, 单位为G
        """

        fds = 0
        try:
            disk = os.statvfs(pcapDir)
            fds = disk.f_frsize/1024.0 * disk.f_bavail/(1024.0*1024.0)
        except Exception, e:
            print e
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )
            return None

        return fds

    def chkDoit(self, lstHost, pdir, minFreeSpaceG):
        """
        删除最早的pcap文件，保障磁盘空间满足最小要求
        """

        try:
            mst0 = pyes.query.TermsQuery(field = "node", value = lstHost)

            if pdir[len(pdir) - 1] == "/":
                dname = pdir + "*"
            else:
                dname = pdir + "/*"
            wild = pyes.query.WildcardQuery(field = "name", value = dname)
            mst1 = pyes.query.BoolQuery(should=wild)
            
            mst_not = pyes.query.TermQuery(field = "locked", value = 1)

            query = pyes.query.BoolQuery(must_not = mst_not)
            query.add_must(mst0)
            query.add_must(mst1)

            search = pyes.query.Search( query=query, start=0, size=20, \
                    sort=[{"first": "asc"}], _source=["num", "name", "first", "size", "node"] )

            # 连接ES
            es = pyes.ES( self._url )
            results = es.search( search, indices="files", doc_types="file" )
            if results != None:
                total = results.total
                if total <= 20:
                    return

                for r in results.hits:
                    if os.path.exists(r._source["name"]) == False:
                        # 删除files索引中的记录
                        es.delete("files", "file", r._id)
                        
                        total = total - 1
                        if total <= 20:
                            break
                    else:
                        freeG = self.getFreeDiskSpace(r._source["name"])
                        if (freeG < minFreeSpaceG):
                            # 删除文件
                            os.remove(r._source["name"]) 
                            # 删除files索引中的记录
                            es.delete("files", "file", r._id)
                            
                            total = total - 1
                            if total <= 20:
                                break
        except Exception, e:
            print e
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )


def checkDevice(pd):
    if pd != None and pd.is_master() == False:
        return

    # 获取主机名
    hostname = socket.gethostname()
    
    chk = chkDevice()
    # 获取主机的结点列表
    lstHost = chk.hostnameToNodeids( hostname )

    # 获取所有结点的pcap文件存储目录列表
    # pcap存储目录可能有多个，用分号分隔开
    dirs = []
    for host in lstHost:
        pcapDirs = chk.getFull( host, "pcapDir", "/srv/pcaps/" )
        for pcapDir in pcapDirs.split(";"):
            pDir = pcapDir.lstrip().rstrip()
            if len(pDir) != 0 and not pDir in dirs:
                dirs.append(pDir)

    for pdir in dirs:
        doit = False
        minFreeSpaceG = 0
        freeG = chk.getFreeDiskSpace(pdir)

        for host in lstHost:
            freeSpaceG = float(chk.getFull( host, "freeSpaceG", 500))
            if freeG < freeSpaceG:
                doit = True

            if freeSpaceG > minFreeSpaceG:
                minFreeSpaceG = freeSpaceG

        if doit == True:
            chk.chkDoit(lstHost, pdir, minFreeSpaceG)

# 测试用
if __name__ == "__main__":
    checkDevice()
