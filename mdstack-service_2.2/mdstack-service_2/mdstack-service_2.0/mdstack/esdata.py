#!/user/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="$2013-08-30"

import os
import pyes
import datetime
import traceback

import postgrestasksched
from utils import sys_config, sys_log, sys_timezone


# TODO
# 该提示在实际部署时要删除
# 恢复被注释掉的LOGPREFIX = "datagroup"
# 注释掉LOGPREFIX = "logstash"

# 索引名格式：datagroup-%Y.%m.%d
LOGPREFIX = "datagroup"
#LOGPREFIX = "logstash"
INDEX_PATTERN = "%Y.%m.%d"

# 时间戳字段
#FIELDTIME = "msg.timestamp"
FIELDTIME = "@timestamp"

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
        self._logFile = conf.getConfig( "tasksched", "logFile" )

        # 实例名
        self._instance = conf.getConfig( "tasksched", "instanceName" )

    def _groupList( self ):
        """
        日志组列表
        """

        postdata = postgrestasksched.PostgresData( self._configFile )
        lst = postdata.getGroupList()
        if lst != None:
            listGroup = [p["groupname"] for p in lst]
        else:
            listGroup = None

        return listGroup

    def _indexList( self, startTime, endTime, limit=0 ):
        """
        索引列表
        startTime: 起始时间
        endTime: 截止时间
        limit: 最多包含多少个时间间隔的索引；默认0，表示全部
        """

        lstIndex = []

        # 索引之间时间间隔: 天
        step_time = 1

        times = 0
        cTime = endTime
        while cTime >= startTime:
            lstIndex.append( LOGPREFIX + "-" + cTime.strftime( INDEX_PATTERN ) )

            times += 1
            if limit > 0 and times >= limit:
                break
            
            cTime += datetime.timedelta( days = 0 - step_time )

        if limit == 0 or times < limit:
            sIndex = LOGPREFIX + "-" + startTime.strftime( INDEX_PATTERN )
            if not ( sIndex in lstIndex ):
                lstIndex.append( sIndex )

        return lstIndex

    def Count( self, searchCond, startTime, endTime ):
        """
        在时间范围[startTime, endTime]内，获取满足条件searchCond的文档数
        """
        
        if startTime == endTime:
            endTime = endTime + datetime.timedelta(seconds = 1)

        cnt = 0
        try:
            lstIndex = self._indexList( startTime, endTime )
            if lstIndex == None or len( lstIndex ) == 0:
                return cnt

            # StringQuery查询
            if searchCond == None or len( searchCond ) == 0:
                searchCond = "*"
            #qry = pyes.query.StringQuery( searchCond, default_field="_all", default_operator="AND" )
            qry = pyes.query.QueryStringQuery( searchCond, default_field="_all", default_operator="AND" )
            # RangeFilter过滤
            ftr = pyes.filters.RangeFilter( pyes.utils.ESRange( FIELDTIME, from_value=sys_timezone.toLocalDatetime(startTime), \
                    to_value=sys_timezone.toLocalDatetime(endTime), include_upper=False ) )
            # FilteredQuery查询
            query = pyes.query.FilteredQuery( qry, ftr )
            search = pyes.query.Search( query = query, start = 0, size = 0, fields = [] )

            # 连接ES
            es = pyes.ES( self._url )
            # 计数
            for index in lstIndex:
                if es.indices.exists_index( index ):
                    #cnt += es.count( query=query, indices=index )['count']
                    cnt += es.search(search, indices=index).total
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )

        return cnt

    def Search( self, searchCond, startTime, endTime, fields=None, start=0, size=50 ):
        """
        在时间范围[startTime, endTime]内，获取满足条件searchCond的文档
        默认按@timestamp倒序排列
        fields: 要返回的字段列表；不指定，返回所有字段；例如：["msg.message","@timestamp"]
        注意：返回的是一个pyes.models.DotDict类型的列表，列表的每一项是一个字典，
        字典中包括以下keys：'sort', '_type', 'fields', '_index', '_score', '_id'
        其中: fields中存储也是一个pyes.models.DotDict，包括的keys就是返回的所有字段
        """

        if startTime == endTime:
            endTime = endTime + datetime.timedelta(seconds = 1)

        lst = []
        try:
            lstIndex = self._indexList( startTime, endTime )
            if lstIndex == None or len( lstIndex ) == 0:
                return lst

            # StringQuery查询
            if searchCond == None or len( searchCond ) == 0:
                searchCond = "*"
            #qry = pyes.query.StringQuery( searchCond, default_field="_all", default_operator="AND" )
            qry = pyes.query.QueryStringQuery( searchCond, default_field="_all", default_operator="AND" )
            # RangeFilter过滤
            ftr = pyes.filters.RangeFilter( pyes.utils.ESRange( FIELDTIME, from_value=sys_timezone.toLocalDatetime(startTime), \
                    to_value=sys_timezone.toLocalDatetime(endTime), include_upper=False ) )
            # FilteredQuery查询
            query = pyes.query.FilteredQuery( qry, ftr )

            # 连接ES
            es = pyes.ES( self._url )
            # 开始查询
            istart = start
            isize = size
            for index in lstIndex:
                if es.indices.exists_index( index ):
                    #search = pyes.query.Search( query=query, start=istart, size=isize, sort=[{FIELDTIME:"desc"}], fields=fields )
                    search = pyes.query.Search( query=query, start=istart, size=isize, sort=[{FIELDTIME:"desc"}], _source=fields )
                    results = es.search( search, indices=index )
                    if results != None:
                        for r in results.hits:
                            lst.append( r )
                        if len( results.hits ) >= isize:
                            break
                        else:
                            isize = isize - len( results.hits )
                            if istart - len( results ) < 0:
                                istart = 0
                            else:
                                istart = istart - len( results )
        except Exception, e:
            sys_log.SysLog( self._logFile, self._instance ).writeLog( "error", str(traceback.format_exc()) )

        return lst


# 测试用
if __name__ == "__main__":
    es = ESData()

    #sCond = 'policy=\'58\''
    sCond = '*'
    sTime = datetime.datetime(2013, 11, 14, 13, 29, 0)
    eTime = datetime.datetime(2013, 11, 14, 13, 34, 0)
    print es.Count(sCond, sTime, eTime)

