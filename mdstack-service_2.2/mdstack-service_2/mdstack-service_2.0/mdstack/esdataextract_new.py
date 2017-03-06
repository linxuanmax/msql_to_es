#!/user/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="$2017-03-01"

import pyes
import datetime
import traceback
import chardet
import os

from utils import sys_config, sys_log, sys_timezone
from elasticsearch import Elasticsearch
from elasticsearch import helpers
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
        self._config = eshost + ':' + esport

        # 日志文件
        self._logFile = conf.getConfig( "dataextract", "logFile" )
        # 实例名
        self._instance = conf.getConfig( "dataextract", "instanceName" )


    def bulkInsData( self, lst, tbstructure, tbname, idxName ):
        """
        lstData:数据    lstFlds:结构   incrFld:主键   tbName:表名   idxName:数据库名称
        """
        try:

            es = Elasticsearch(self._config)
            actions = []
            action = {}
            for data in lst:
                souce = dict(zip(tbstructure, data))
                action['_index'] = tbname
                action['_type'] = idxName
                action['_id'] = data[0]
                action['_source'] = souce
                print action
                actions.append(action)
                if (len(actions) > 0):
                    helpers.bulk(es, actions)



        except Exception, e:
            #print '--------',e
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )



