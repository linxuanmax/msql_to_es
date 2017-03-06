#!/user/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="$2013-09-17"

import pyes
import datetime
import traceback
import chardet
import os
from utils import sys_config, sys_log, sys_timezone

# 索引名格式：idx_表名
LOGPREFIX = "idx_"

# 字段列表
FIELDMSG = "msg.message"
FIELDTIME = "@timestamp"
FIELDGROUP = "msg.group"
FIELDHOST = "msg.host"
FIELDDN = "msg.dn"

# 批量插入数据时，每批的数据量
BULKSIZE = 2

# 配置文件名称
CONFIG_FILE = "mdstack.conf"

class ESData():
    """
    elasticsearch数据处理类
    """

    def __init__( self, cfgFile = None ):
        self._configFile = cfgFile
        if self._configFile == None:
            #self._configFile = sys_config.getDir() + "/conf/" + CONFIG_FILE
            self._configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/" + CONFIG_FILE
            if os.path.exists(self._configFile) == False:
                self._configFile = "/opt/mdstack/conf/mdstackd/" + CONFIG_FILE

        conf = sys_config.SysConfig( self._configFile )

        eshost = conf.getConfig( "elasticsearch", "esHost" )
        esport = conf.getConfig( "elasticsearch", "esPort" )
        self._url = 'http://' + eshost + ":" + esport
        # 日志文件
        self._logFile = conf.getConfig( "dataextract", "logFile" )

        # 实例名
        self._instance = conf.getConfig( "dataextract", "instanceName" )


    def bulkInsData( self, lstData, incType, timeFld, lstFlds, incrFld, tbName, idxName ):
        """
        lstFlds中有dbid, fldsrc, fldout关键字
        在向ES写入数据时，必须包含以下字段：
            tbname 表名
        """
        fldLen = len( lstFlds )
        try:
            # 连接ES
            es = pyes.ES( self._url, bulk_size=BULKSIZE )
            idxName = LOGPREFIX + idxName

            #如果没有则默认为表名
            typeName = tbName
                        #表名tbName
            for dt in lstData: # lstData返回的是元组列表，而不是lstFlds那种字典列表
                docid = None
                data = {"tbname": tbName, "lastmoditime___": datetime.datetime.now()}
                for i in xrange(fldLen):
                    # 空值不导入
                    if dt[i] is not None:
                        if isinstance(dt[i], datetime.time):
                            data[lstFlds[i]["fldsrc"]] = (dt[i]).strftime("%H:%M:%S")
                        else:
                            data[lstFlds[i]["fldsrc"]] = dt[i]
                        if (lstFlds[i]["fldsrc"]).lower() == incrFld.lower(): docid = dt[i]
                        if incType == 2 and (lstFlds[i]["fldsrc"]).lower() == timeFld.lower(): data["lastmoditime___"] = dt[i]
                #print data
                #print idxName, '=======', data
                es.index( data, idxName, typeName, docid, bulk=True )
            #es.refresh()
            es.force_bulk()
        except Exception, e:
            #print '--------',e
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )



