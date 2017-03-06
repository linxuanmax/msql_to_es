#!/user/bin/env python
# -*- coding=utf-8 -*-

__author__="fallmoon"
__date__="$2013-10-23"

import os
import pyes
import datetime
import traceback

from utils import sys_config, sys_log, sys_timezone


# 日志索引名格式：datagroup-%Y.%m.%d
LOGPREFIX = "datagroup"
INDEX_PATTERN = "%Y.%m.%d"

# 日志索引时间戳字段
FIELDTIME = "@timestamp"

# 流量索引名格式：sessions-%Y.%m.%d
FLOWS_LOGPREFIX = "sessions"
FLOWS_INDEX_PATTERN = "%y%m%d"

# 流量索引时间戳字段
FLOWS_FIELDTIME = "lpd"

# 流量采集结点字段
FIELD_NODE = "no"

# 配置文件名称
CONFIG_FILE = "mdstack.conf"

class ESData():
    """
    elasticsearch数据处理类
    """

    def __init__(self, cfgFile = None):
        self._configFile = cfgFile
        if self._configFile == None:
            #self._configFile = sys_config.getDir() + "/conf/" + CONFIG_FILE
            self._configFile = os.path.split(os.path.realpath(__file__))[0] + "/conf/" + CONFIG_FILE
            if os.path.exists(self._configFile) == False:
                self._configFile = "/opt/mdstack/conf/mdstackd/" + CONFIG_FILE

        conf = sys_config.SysConfig(self._configFile)

        eshost = conf.getConfig("elasticsearch", "esHost")
        esport = conf.getConfig("elasticsearch", "esPort")
        self._url = 'http://' + eshost + ":" + esport

        # 日志文件
        self._logFile = conf.getConfig("datastats", "logFile")

        # 实例名
        self._instance = conf.getConfig("datastats", "instanceName")

    
    def Delete_Index(self, dDate = None, idxType = "logs"):
        """
        删除某天的索引
        """
        
        if dDate == None:
            dDate = datetime.datetime.now()

        idxname = LOGPREFIX + "-" + dDate.strftime(INDEX_PATTERN)
        if idxType == "flows":
            idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)

        # 连接ES
        es = pyes.ES(self._url)
        es.indices.delete_index_if_exists(idxname)
        #if es.indices.exists_index(idxname):
        #    es.indices.delete_index(idxname)


    def Delete_Overdue_Indexes(self, retain, dDate = None, idxType = "logs"):
        """
        删除已过期的索引
        """

        if retain <= 0:
            return

        if dDate == None:
            dDate = datetime.datetime.now()
        dDate = dDate + datetime.timedelta(days = 0 - retain)
        
        idxname = LOGPREFIX + "-" + dDate.strftime(INDEX_PATTERN)
        idxPattern = LOGPREFIX + "-*"
        if idxType == "flows":
            idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)
            idxPattern = FLOWS_LOGPREFIX + "-*"

        # 连接ES
        es = pyes.ES(self._url)
        # 获取索引列表
        lst = es.indices.aliases(idxPattern).keys()
        for idx in lst:
            if idx < idxname:
                es.indices.delete_index(idx)


    def Optimize(self, dDate = None, idxType = "logs"):
        """
        优化某天的索引
        """

        if dDate == None:
            dDate = datetime.datetime.now()

        idxname = LOGPREFIX + "-" + dDate.strftime(INDEX_PATTERN)
        if idxType == "flows":
            idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)

        # 连接ES
        es = pyes.ES(self._url)
        if es.indices.exists_index(idxname):
            es.indices.optimize(idxname)


    def Exists(self, dDate = None, idxType = "logs"):
        """
        判断某天是否有索引
        """

        if dDate == None:
            dDate = datetime.datetime.now()

        idxname = LOGPREFIX + "-" + dDate.strftime(INDEX_PATTERN)
        if idxType == "flows":
            idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)

        # 连接ES
        es = pyes.ES(self._url)
        return es.indices.exists_index(idxname)


    def _indexList(self, startTime, endTime, limit = 0, idxType = "logs"):
        """
        索引列表
        startTime: 起始时间
        endTime: 截止时间
        limit: 最多包含多少个时间间隔的索引；默认0，表示全部
        """

        lstIndex = []
        
        logPrefix = LOGPREFIX
        idxPattern = INDEX_PATTERN
        if idxType == "flows":
            logPrefix = FLOWS_LOGPREFIX
            idxPattern = FLOWS_INDEX_PATTERN

        # 索引之间时间间隔: 天
        step_time = 1

        times = 0
        cTime = endTime
        while cTime >= startTime:
            lstIndex.append(logPrefix + "-" + cTime.strftime(idxPattern))

            times += 1
            if limit > 0 and times >= limit:
                break

            cTime += datetime.timedelta(days = 0 - step_time)

        if limit == 0 or times < limit:
            sIndex = logPrefix + "-" + startTime.strftime(idxPattern)
            if not (sIndex in lstIndex):
                lstIndex.append(sIndex)

        return lstIndex


    def Count(self, startTime, endTime, searchCond = None, idxType = "logs"):
        """
        在时间范围[startTime, endTime]内，获取满足条件searchCond的文档数
        注意：此函数用作统计,起始时间和截止时间应该在同一天
        """
        
        if startTime == endTime:
            endTime = endTime + datetime.timedelta(seconds = 1)

        cnt = 0
        try:
            fldTime = FIELDTIME
            if idxType == "flows":
                fldTime = FLOWS_FIELDTIME

            lstIndex = self._indexList(startTime, endTime, idxType = idxType)
            if lstIndex == None or len(lstIndex) == 0:
                return cnt

            # StringQuery查询
            if searchCond == None or len(searchCond) == 0:
                searchCond = "*"
            #qry = pyes.query.StringQuery(searchCond, default_field="_all", default_operator="AND")
            qry = pyes.query.QueryStringQuery(searchCond, default_field="_all", default_operator="AND")
            # RangeFilter过滤
            ftr = pyes.filters.RangeFilter(pyes.utils.ESRange(fldTime, from_value=sys_timezone.toLocalDatetime(startTime), \
                    to_value=sys_timezone.toLocalDatetime(endTime), include_upper=False))
            # FilteredQuery查询
            query = pyes.query.FilteredQuery(qry, ftr)
            search = pyes.query.Search( query = query, start = 0, size = 0, fields = [] )

            # 连接ES
            es = pyes.ES(self._url)
            # 计数
            for index in lstIndex:
                if es.indices.exists_index(index):
                    #cnt += es.count(query=query, indices=index)['count']
                    cnt += es.search(search, indices=index).total
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))

        return cnt

    def Sum(self, startTime, endTime, sumfield, searchCond = None, idxType = "logs"):
        """
        在时间范围[startTime, endTime]内，统计某个数字型字段的和
        注意：此函数用作统计,起始时间和截止时间应该在同一天
        sumfield：必须为数字型字段
        """

        if startTime == endTime:
            endTime = endTime + datetime.timedelta(seconds = 1)

        ret = 0
        try:
            fldTime = FIELDTIME
            if idxType == "flows":
                fldTime = FLOWS_FIELDTIME

            lstIndex = self._indexList(startTime, endTime, idxType = idxType)
            if lstIndex == None or len(lstIndex) == 0:
                return ret

            # StringQuery查询
            if searchCond == None or len(searchCond) == 0:
                searchCond = "*"
            qry = pyes.query.QueryStringQuery(searchCond, default_field="_all", default_operator="AND")
            # RangeFilter过滤
            ftr = pyes.filters.RangeFilter(pyes.utils.ESRange(fldTime, from_value=sys_timezone.toLocalDatetime(startTime), \
                    to_value=sys_timezone.toLocalDatetime(endTime), include_upper=False))
            # FilteredQuery查询
            query = pyes.query.FilteredQuery(qry, ftr)
            # Search
            search = query.search()
            # Sum aggregation
            sumagg = pyes.aggs.SumAgg(name = "tmpagg", field = sumfield)
            search.agg.add(sumagg)

            # 连接ES
            es = pyes.ES(self._url)
            # 计数
            for index in lstIndex:
                if es.indices.exists_index(index):
                    result = es.search(search, size = 0, indices = index)
                    ret += int(result.aggs.tmpagg.value)
        except Exception, e:
            sys_log.SysLog(self._logFile, self._instance).writeLog("error", str(traceback.format_exc()))

        return ret

    def SizeIndex(self, dDate = None, idxType = "logs"):
        """
        获取某天的索引数据空间大小
        """

        size = 0

        if dDate == None:
            dDate = datetime.datetime.now() - datetime.timedelta(days = 1)

        idxname = LOGPREFIX + "-" + dDate.strftime(INDEX_PATTERN)
        if idxType == "flows":
            idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)

        # 连接ES
        es = pyes.ES(self._url)
        if es.indices.exists_index(idxname):
            # idxstats = es.index_stats(idxname)
            idxstats = es.indices.stats(idxname)
            size = idxstats['_all']['total']['store']['size_in_bytes']

        return size

    def Get_node_list_day(self, dDate = None):
        """
        获取某天流量数据的采集结点列表
        """

        lst = []
        if dDate == None:
            dDate = datetime.datetime.now()
        idxname = FLOWS_LOGPREFIX + "-" + dDate.strftime(FLOWS_INDEX_PATTERN)

        # 连接ES
        es = pyes.ES(self._url)
        if es.indices.exists_index(idxname):
            # 查询
            query = pyes.query.MatchAllQuery()
            search = query.search()

            # terms aggregation
            terms_agg = pyes.aggs.TermsAgg(name = "tmpagg", field = FIELD_NODE, size = 0)
            search.agg.add(terms_agg)

            result = es.search(search, size = 0, indices = idxname)
            lst = result.aggs.tmpagg.buckets

        return lst


# 测试用
if __name__ == "__main__":
    es = ESData()
     
    print es.Exists()
    print es.Exists(datetime.date(2014, 8, 28), idxType = "flows")

    sTime = datetime.datetime(2014, 8, 28, 0, 0, 0)
    eTime = datetime.datetime(2014, 8, 28, 23, 59, 59)
    print es.Count(sTime, eTime, idxType = "flows")

    print es.SizeIndex()
    print es.SizeIndex(datetime.date(2014, 8, 28), idxType = "flows")

    """
    print es.Count(sTime, eTime, "src.ip:\"192.168.0.4\"")
    print es.Count(sTime, eTime, "type: \"syslog\"")

    print es.Exists(datetime.date(2012, 12, 5))

    es.Delete_Overdue_Indexes(1, datetime.date(2012, 12, 6))

    print es.Exists(datetime.date(2012, 12, 5))

    es.Delete_Overdue_Indexes(1, datetime.date(2012, 12, 7))

    print es.Exists(datetime.date(2012, 12, 5))
    """
